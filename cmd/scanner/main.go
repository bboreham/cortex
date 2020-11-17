package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	pageCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "pages_scanned_total",
		Help:      "Total count of pages scanned from a table",
	}, []string{"table"})
	chunksPerUser = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_chunks_stored_total",
		Help: "Total stored chunks per user.",
	}, []string{"user"})
	chunksNextPerUser = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_chunks_nextweek_total",
		Help: "Total chunks from next week copied per user.",
	}, []string{"user"})
	chunkSizePerUser = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_chunk_stored_bytes_total",
		Help: "Total bytes stored in chunks per user.",
	}, []string{"user"})

	removeBogusKubeletMetrics bool
	copyChunksForNextWeek     bool
	minChunkLength            int
	endOfWeek                 model.Time
	hourLimitStart            int
	hourLimitEnd              int
)

func main() {
	var (
		schemaConfig     chunk.SchemaConfig
		rechunkConfig    chunk.SchemaConfig
		storageConfig    storage.Config
		chunkStoreConfig chunk.StoreConfig
		encodingConfig   encoding.Config
		tbmConfig        chunk.TableManagerConfig
		limitsConfig     validation.Limits

		deleteOrgsFile string
		includeOrgsStr string

		week          int64
		segments      int
		totalSegments int
		startSegment  int
		loglevel      string
		address       string

		chunkReadCapacity int64
		indexReadCapacity int64
		rechunkSchemaFile string
	)

	flagext.RegisterFlags(&storageConfig, &schemaConfig, &chunkStoreConfig, &encodingConfig, &tbmConfig, &limitsConfig)
	flag.StringVar(&address, "address", ":6060", "Address to listen on, for profiling, etc.")
	flag.Int64Var(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.Int64Var(&chunkReadCapacity, "chunk-read-provision", 1000, "DynamoDB read provision for chunk table")
	flag.Int64Var(&indexReadCapacity, "index-read-provision", 1000, "DynamoDB read provision for chunk table")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
	flag.IntVar(&totalSegments, "totalSegments", 0, "Number of segments to split table into (if zero then use number of parallel segments)")
	flag.IntVar(&startSegment, "startSegment", 0, "Segment number to start at (useful after failed job)")
	flag.StringVar(&deleteOrgsFile, "delete-orgs-file", "", "File containing IDs of orgs to delete")
	flag.StringVar(&includeOrgsStr, "include-orgs", "", "IDs of orgs to include (space-separated)")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.StringVar(&rechunkSchemaFile, "rechunk-yaml", "", "Yaml definition of new chunk tables (blank to disable)")
	flag.BoolVar(&removeBogusKubeletMetrics, "remove-bogus-kubelet", false, "Remove bogus Kubelete metrics from the output")
	flag.IntVar(&minChunkLength, "min-chunk-length", 0, "Drop chunks smaller than this size")
	flag.BoolVar(&copyChunksForNextWeek, "copy-next-week", false, "Copy in chunks that span into next week")
	flag.IntVar(&hourLimitStart, "active-hours-start", 0, "Hour number [0-23] when we can start running")
	flag.IntVar(&hourLimitEnd, "active-hours-end", 0, "Hour number [0-23] when we must stop running")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	// HTTP listener for profiling
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		checkFatal(http.ListenAndServe(address, nil))
	}()

	deleteOrgs, includeOrgs := setupOrgs(deleteOrgsFile, includeOrgsStr)

	secondsInWeek := int64(7 * 24 * time.Hour.Seconds())
	if week == 0 {
		week = time.Now().Unix() / secondsInWeek
	}
	tableTime := model.TimeFromUnix(week * secondsInWeek)
	endOfWeek = tableTime.Add(7 * 24 * time.Hour)

	err := schemaConfig.Load()
	checkFatal(err)

	overrides, err := validation.NewOverrides(limitsConfig)
	checkFatal(err)
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	checkFatal(err)
	defer chunkStore.Stop()

	var reindexStore chunk.Store
	var tm *tableManager
	if rechunkSchemaFile != "" {
		err := rechunkConfig.LoadFromFile(rechunkSchemaFile)
		checkFatal(err)
		if len(rechunkConfig.Configs) != 1 {
			checkFatal(fmt.Errorf("rechunk config must have 1 config"))
		}
		reindexStore, err = storage.NewStore(storageConfig, chunkStoreConfig, rechunkConfig, overrides)
		checkFatal(err)
		tm, err = setupTableManager(tbmConfig, storageConfig, rechunkConfig, tableTime)
		checkFatal(err)
	}

	tableName, err := schemaConfig.ChunkTableFor(tableTime)
	checkFatal(err)
	fmt.Printf("table %s\n", tableName)
	indexTableName, err := schemaConfig.IndexTableFor(tableTime)
	checkFatal(err)

	readClient, err := storage.NewTableClient("aws", storageConfig)
	checkFatal(err)
	prevReadCapacity, err := setReadCapacity(context.Background(), readClient, tableName, chunkReadCapacity)
	checkFatal(err)
	prevIndexReadCapacity, err := setReadCapacity(context.Background(), readClient, indexTableName, indexReadCapacity)
	checkFatal(err)

	if tm != nil {
		tm.WaitForAllActive(context.Background(), tableTime.Time(), tableTime.Time())
	}

	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		// This only works for series-store, i.e. schema v9 and v10
		handlers[segment] = newHandler(chunkStore.(chunk.Store2), reindexStore.(chunk.Store2), indexTableName, includeOrgs, deleteOrgs)
		callbacks[segment] = handlers[segment].handlePage
	}
	if totalSegments == 0 {
		totalSegments = segments
	}

	err = chunkStore.(chunk.Store2).Scan(context.Background(), tableTime, tableTime, false, startSegment, totalSegments, callbacks)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error from Scan", "err", err)
	}

	level.Info(util.Logger).Log("msg", "finished, shutting down")
	if reindexStore != nil {
		reindexStore.Stop()
	}
	// Set back as it was before
	_, err = setReadCapacity(context.Background(), readClient, tableName, prevReadCapacity)
	checkFatal(err)
	_, err = setReadCapacity(context.Background(), readClient, indexTableName, prevIndexReadCapacity)
	checkFatal(err)
	if tm != nil {
		tm.Stop()
		// Sync tables as-at two weeks after, which should set them to inactive throughput
		err = tm.SyncTables(context.Background(), tableTime.Time().Add(2*7*24*time.Hour), tableTime.Time())
		if err != nil {
			level.Error(util.Logger).Log("msg", "error syncing tables", "err", err)
		}
	}

	totals := newSummary()
	for segment := 0; segment < segments; segment++ {
		totals.accumulate(handlers[segment].summary)
	}
	totals.print(deleteOrgs)

	time.Sleep(20 * time.Second) // get one more scrape in for final metrics
}

// wrap the "real" TableManager
type tableManager struct {
	*chunk.TableManager
	done chan struct{}
}

func setupTableManager(tbmConfig chunk.TableManagerConfig, storageConfig storage.Config, rechunkConfig chunk.SchemaConfig, tableTime model.Time) (*tableManager, error) {
	// We want our table-manager to manage just a one-week period
	rechunkConfig.Configs[0].From.Time = tableTime
	tbmConfig.CreationGracePeriod = time.Hour * 1

	{
		// First, run a pass with no metrics autoscaling, so we just get max throughput
		scNoMetrics := storageConfig
		scNoMetrics.AWSStorageConfig.Metrics.URL = ""
		tcNoMetrics, err := storage.NewTableClient(rechunkConfig.Configs[0].IndexType, scNoMetrics)
		if err != nil {
			return nil, errors.Wrap(err, "creating table client")
		}
		tmNoMetrics, err := chunk.NewTableManager(tbmConfig, rechunkConfig, 0, tcNoMetrics, nil)
		if err != nil {
			return nil, errors.Wrap(err, "initializing table manager")
		}
		err = tmNoMetrics.SyncTables(context.Background(), tableTime.Time(), tableTime.Time())
		if err != nil {
			return nil, errors.Wrap(err, "sync tables")
		}
	}

	// Now go back and create client and TableManager for real
	// We need to "trick" the metrics auto-scaling into scaling up and down,
	// even though we don't have a queue that it's used to looking at.
	// Pretend we have a queue that is always enormous and also always shrinking
	trickQuery := "2000000000-timestamp(count(up))"
	storageConfig.AWSStorageConfig.Metrics.QueueLengthQuery = trickQuery
	// Reduce query window from 15m to 2m so we don't blur things so much
	storageConfig.AWSStorageConfig.Metrics.UsageQuery = `sum(rate(cortex_dynamo_consumed_capacity_total{operation="DynamoDB.BatchWriteItem"}[2m])) by (table) > 0`

	tableClient, err := storage.NewTableClient(rechunkConfig.Configs[0].IndexType, storageConfig)
	if err != nil {
		return nil, errors.Wrap(err, "creating table client")
	}
	tm := &tableManager{
		done: make(chan struct{}),
	}
	tm.TableManager, err = chunk.NewTableManager(tbmConfig, rechunkConfig, 0, tableClient, nil)
	if err != nil {
		return nil, errors.Wrap(err, "initializing table manager")
	}
	// Sync continuously in background
	go tm.loop(tableTime.Time())
	return tm, nil
}

func setReadCapacity(ctx context.Context, client chunk.TableClient, tableName string, val int64) (int64, error) {
	current, _, err := client.DescribeTable(ctx, tableName)
	if err != nil {
		return 0, err
	}
	prevReadCapacity := current.ProvisionedRead
	if current.UseOnDemandIOMode {
		prevReadCapacity = 0
	}
	updated := current
	if val > 0 {
		updated.ProvisionedRead = val
		updated.UseOnDemandIOMode = false
	} else {
		updated.UseOnDemandIOMode = true
	}
	if updated.ProvisionedWrite == 0 {
		updated.ProvisionedWrite = 1
	}
	err = client.UpdateTable(ctx, current, updated)
	return prevReadCapacity, err
}

func (m *tableManager) Stop() {
	// Send on chan rather than close() so we don't return until the send is received
	m.done <- struct{}{}
}

// Sync like the real table-manager does, but at the specific time we are operating
func (m *tableManager) loop(atTime time.Time) {
	time.Sleep(6 * time.Minute) // wait a bit to allow perf to settle
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.SyncTables(ctx, atTime, atTime); err != nil {
				level.Error(util.Logger).Log("msg", "error syncing tables", "err", err)
			}
		case <-m.done:
			return
		}
	}
}

func setupOrgs(deleteOrgsFile, includeOrgsStr string) (deleteOrgs, includeOrgs map[int]struct{}) {
	deleteOrgs = map[int]struct{}{}
	if deleteOrgsFile != "" {
		content, err := ioutil.ReadFile(deleteOrgsFile)
		checkFatal(err)
		for _, arg := range strings.Fields(string(content)) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			deleteOrgs[org] = struct{}{}
		}
	}

	includeOrgs = map[int]struct{}{}
	if includeOrgsStr != "" {
		for _, arg := range strings.Fields(includeOrgsStr) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			includeOrgs[org] = struct{}{}
		}
	}
	return
}

/* TODO: delete v8 schema rows for all instances */

type summary struct {
	// Map from instance to (metric->count)
	counts map[int]map[string]int
}

func newSummary() summary {
	return summary{
		counts: map[int]map[string]int{},
	}
}

func (s *summary) accumulate(b summary) {
	for instance, m := range b.counts {
		if s.counts[instance] == nil {
			s.counts[instance] = make(map[string]int)
		}
		for metric, c := range m {
			s.counts[instance][metric] += c
		}
	}
}

func (s summary) print(deleteOrgs map[int]struct{}) {
	for instance, m := range s.counts {
		deleted := ""
		if _, found := deleteOrgs[instance]; found {
			deleted = "deleted"
		}
		for metric, c := range m {
			fmt.Printf("%d, %s, %d, %s\n", instance, metric, c, deleted)
		}
	}
}

type handler struct {
	readStore   chunk.Store2
	writeStore  chunk.Store2
	tableName   string
	pages       int
	includeOrgs map[int]struct{}
	deleteOrgs  map[int]struct{}
	summary
}

func newHandler(readStore, writeStore chunk.Store2, tableName string, includeOrgs map[int]struct{}, deleteOrgs map[int]struct{}) handler {
	if len(includeOrgs) == 0 {
		includeOrgs = nil
	}
	return handler{
		readStore:   readStore,
		writeStore:  writeStore,
		tableName:   tableName,
		includeOrgs: includeOrgs,
		deleteOrgs:  deleteOrgs,
		summary:     newSummary(),
	}
}

// ReadBatchHashIterator is an iterator over a ReadBatch with a HashValue method.
type ReadBatchHashIterator interface {
	Next() bool
	RangeValue() []byte
	Value() []byte
	HashValue() string
}

func (h *handler) handlePage(page chunk.ReadBatch) {
	ctx := context.Background()

	if hourLimitStart != 0 && hourLimitEnd != 0 {
		for {
			hourNow := time.Now().Hour()
			if hourNow >= hourLimitStart && hourNow <= hourLimitEnd {
				break
			}
			time.Sleep(time.Minute)
		}
	}

	pageCounter.WithLabelValues(h.tableName).Inc()
	for i := page.Iterator().(ReadBatchHashIterator); i.Next(); {
		if !isRecognisedRecord(i.RangeValue()) {
			continue
		}
		org, orgStr, seriesID, from, through, err := decodeHashValue(i.HashValue())
		if err != nil {
			level.Error(util.Logger).Log("msg", "error in hash value", "hash", i.HashValue())
			continue
		}
		if h.includeOrgs != nil {
			if _, found := h.includeOrgs[org]; !found {
				continue
			}
		}
		if h.counts[org] == nil {
			h.counts[org] = make(map[string]int)
		}
		h.counts[org][""]++
		if _, found := h.deleteOrgs[org]; found {
			continue
		}
		if h.writeStore.DoneThisSeriesBefore(from, through, orgStr, seriesID) {
			continue
		}

		newChunk, extras, allZeroes, err := dedupe(h.readStore, orgStr, seriesID, from, through)
		if err != nil {
			level.Error(util.Logger).Log("msg", "chunk dedupe error", "err", err)
			continue
		}
		metricName := newChunk.Metric.Get(labels.MetricName)
		switch {
		case isBogus(org, newChunk.Metric, allZeroes):
			h.counts[org][metricName+"-bogus"]++
		case newChunk.Data.Len() < minChunkLength:
			h.counts[org][metricName+"-bogus-tiny"]++
		default:
			// Check again in case another thread completed this one while we were reading
			if h.writeStore.DoneThisSeriesBefore(from, through, orgStr, seriesID) {
				continue
			}
			h.putWithRetry(ctx, newChunk)
			chunksPerUser.WithLabelValues(orgStr).Inc()
			chunkSizePerUser.WithLabelValues(orgStr).Add(float64(newChunk.Data.Size()))
			h.counts[org][metricName]++

			// Copy through all chunks that span into next table, for when we stop re-indexing
			for _, c := range extras {
				h.putNoIndexWithRetry(ctx, c)
			}
			chunksNextPerUser.WithLabelValues(orgStr).Add(float64(len(extras)))
		}
		// This cache write may duplicate what the store did, but we can't
		// guarantee it's v9+, and don't know we have the same series IDs as it has
		h.writeStore.MarkThisSeriesDone(context.TODO(), from, through, orgStr, seriesID)
	}
}

func (h *handler) putWithRetry(ctx context.Context, newChunk *chunk.Chunk) {
	for c := 0; c < 100; c++ {
		err := h.writeStore.Put(ctx, []chunk.Chunk{*newChunk})
		if err == nil {
			return
		}
		level.Error(util.Logger).Log("msg", "put error - retrying", "err", err, "chunk", newChunk.Description())
		time.Sleep(time.Second)
	}
	level.Error(util.Logger).Log("msg", "giving up after 1000 retries")
	os.Exit(1)
}

func (h *handler) putNoIndexWithRetry(ctx context.Context, chunk chunk.Chunk) {
	for c := 0; c < 100; c++ {
		err := h.writeStore.PutNoIndex(ctx, chunk)
		if err == nil {
			return
		}
		level.Error(util.Logger).Log("msg", "putNoIndex error - retrying", "err", err, "chunk", chunk.Description())
		time.Sleep(time.Second)
	}
	level.Error(util.Logger).Log("msg", "giving up after 1000 retries")
	os.Exit(1)
}

func isRecognisedRecord(rangeValue []byte) bool {
	const chunkTimeRangeKeyV3 = '3'
	return len(rangeValue) > 2 && rangeValue[len(rangeValue)-2] == chunkTimeRangeKeyV3
}

func decodeHashValue(hashValue string) (org int, orgStr, seriesID string, from, through model.Time, err error) {
	hashParts := strings.SplitN(hashValue, ":", 3)
	if len(hashParts) != 3 {
		err = fmt.Errorf("unrecognized hash value: %q", hashValue)
		return
	}
	orgStr = hashParts[0]
	seriesID = hashParts[2]
	org, err = strconv.Atoi(orgStr)
	if err != nil {
		err = fmt.Errorf("unrecognized org string: %s", err)
		return
	}
	from, through, err = decodeDayNumber(hashParts[1])
	return
}

func decodeDayNumber(day string) (model.Time, model.Time, error) {
	if len(day) < 2 || day[0] != 'd' {
		return 0, 0, fmt.Errorf("invalid number: %q", day)
	}
	dayNumber, err := strconv.Atoi(day[1:])
	if err != nil {
		return 0, 0, err
	}
	const millisecondsInDay = model.Time(24 * time.Hour / time.Millisecond)
	// Fetch the whole day that this date is in
	from := model.Time(dayNumber) * millisecondsInDay
	// Time intervals are inclusive, so step back one millisecond from the next day
	through := from + millisecondsInDay - 1
	return from, through, nil
}

func dedupe(dstore chunk.Store2, userID, seriesID string, from, through model.Time) (*chunk.Chunk, []chunk.Chunk, bool, error) {
	// FIXME: this shouldn't really be necessary, but store query APIs rely on it
	ctx := user.InjectOrgID(context.Background(), userID)
	chunks, err := dstore.AllChunksForSeries(ctx, userID, seriesID, from, through)
	if err != nil {
		return nil, nil, false, err
	}
	if len(chunks) == 0 {
		return nil, nil, false, fmt.Errorf("no chunks found for %s:%s for %v-%v", userID, seriesID, from, through)
	}
	data, first, last, allZeroes, err := dataFromChunks(from, through, chunks)
	if err != nil {
		return nil, nil, false, err
	}
	//level.Info(util.Logger).Log("msg", "new chunk", "userID", userID, "fp", chunks[0].Fingerprint, "metric", chunks[0].Metric, "first", first, "last", last)
	ret := &chunk.Chunk{
		Fingerprint: chunks[0].Fingerprint,
		UserID:      userID,
		From:        model.Time(first),
		Through:     model.Time(last),
		Metric:      chunks[0].Metric,
		Encoding:    encoding.Bigchunk,
		Data:        data,
	}
	err = ret.Encode()
	if err != nil {
		return nil, nil, false, err
	}
	var extras []chunk.Chunk
	if copyChunksForNextWeek {
		for _, c := range chunks {
			if c.Through > endOfWeek {
				extras = append(extras, c)
			}
		}
	}
	return ret, extras, allZeroes, nil
}

const timestampTolerance = 5

// unpack and dedupe all samples from previous chunks; create one new chunk.
func dataFromChunks(from, through model.Time, chunks []chunk.Chunk) (ret encoding.Chunk, first, last int64, allZeroes bool, err error) {
	ret, err = encoding.NewForEncoding(encoding.Bigchunk)
	if err != nil {
		return
	}
	iter := batch.NewChunkMergeIterator(chunks, 0, 0)
	if !iter.Seek(int64(from)) {
		err = fmt.Errorf("Could not seek to start time")
		return
	}
	first, _ = iter.At()
	allZeroes = true
	for {
		ts, v := iter.At()
		if ts > int64(through) {
			break
		}
		// If gap since last scrape is very close to an exact number of seconds, tighten it up
		if last != 0 {
			gap := ts - last
			seconds := ((gap + 500) / 1000)
			diff := int(gap - seconds*1000)
			// Don't go past 'through' limit.
			if diff != 0 && diff >= -timestampTolerance && diff <= timestampTolerance && last+seconds*1000 <= int64(through) {
				ts = last + seconds*1000
			}
		}
		last = ts
		if v != 0 {
			allZeroes = false
		}
		ret.Add(model.SamplePair{Timestamp: model.Time(ts), Value: model.SampleValue(v)})
		if !iter.Next() {
			break
		}
	}
	return
}

func isBogus(org int, lbls labels.Labels, allZeroes bool) bool {
	if !removeBogusKubeletMetrics {
		return false
	}
	metricName := lbls.Get(labels.MetricName)
	if strings.HasPrefix(metricName, "container_") {
		// Drop metrics which are disabled but still sent as all zeros by kubelet
		if metricName == "container_network_tcp_usage_total" ||
			metricName == "container_network_udp_usage_total" ||
			metricName == "container_tasks_state" ||
			metricName == "container_cpu_load_average_10s" {
			return true
		}
		id := lbls.Get("id")
		if strings.HasPrefix(id, "/system.slice") {
			// Drop series for systemd tasks which are all-zero
			if allZeroes {
				return true
			}
			// Drop all cAdvisor metrics for leaked systemd tasks
			if strings.HasPrefix(id, "/system.slice/run-") && strings.HasSuffix(id, "scope") {
				return true
			}
		}
	}
	if metricName == "kube_configmap_metadata_resource_version" {
		return true
	}
	// Reduce bucket density - from https://github.com/coreos/kube-prometheus/blob/48d95f0b9fc9/manifests/prometheus-serviceMonitorApiserver.yaml
	if metricName == "apiserver_request_duration_seconds_bucket" {
		le := lbls.Get("le")
		if le == "0.15" || le == "0.25" || le == "0.3" || le == "0.35" || le == "0.4" || le == "0.45" || le == "0.6" || le == "0.7" || le == "0.8" || le == "0.9" || le == "1.25" || le == "1.5" || le == "1.75" || le == "2.5" || le == "3" || le == "3.5" || le == "4.5" || le == "6" || le == "7" || le == "8" || le == "9" || le == "15" || le == "25" || le == "30" || le == "50" {
			return true
		}
	}
	// Bogus metrics from old api-server
	if strings.HasPrefix(metricName, "apiserver_admission_controller_admission_latencies_seconds_") ||
		strings.HasPrefix(metricName, "apiserver_admission_step_admission_latencies_seconds_") {
		return true
	}
	// Obsolete kubelet metrics - from https://github.com/coreos/kube-prometheus/blob/a8b4985de4dc/jsonnet/kube-prometheus/dropping-deprecated-metrics-relabelings.libsonnet
	if strings.HasPrefix(metricName, "kubelet_") {
		if metricName == "kubelet_pod_worker_latency_microseconds" ||
			metricName == "kubelet_pod_start_latency_microseconds" ||
			metricName == "kubelet_cgroup_manager_latency_microseconds" ||
			metricName == "kubelet_pod_worker_start_latency_microseconds" ||
			metricName == "kubelet_pleg_relist_latency_microseconds" ||
			metricName == "kubelet_pleg_relist_interval_microseconds" ||
			metricName == "kubelet_runtime_operations" ||
			metricName == "kubelet_runtime_operations_latency_microseconds" ||
			metricName == "kubelet_runtime_operations_errors" ||
			metricName == "kubelet_eviction_stats_age_microseconds" ||
			metricName == "kubelet_device_plugin_registration_count" ||
			metricName == "kubelet_device_plugin_alloc_latency_microseconds" ||
			metricName == "kubelet_network_plugin_operations_latency_microseconds" ||
			metricName == "kubelet_docker_operations" ||
			metricName == "kubelet_docker_operations_latency_microseconds" ||
			metricName == "kubelet_docker_operations_errors" ||
			metricName == "kubelet_docker_operations_timeout" {
			return true
		}
	}
	// Metric names which are so long and convoluted no-one is going to use them
	if len(metricName) > 100 && strings.Count(metricName, "_") > 12 {
		return true
	}
	return false
}

func removeBlanks(a *labels.Labels) {
	for i := 0; i < len(*a); {
		if len((*a)[i].Value) == 0 {
			// Delete by moving up remaining elements
			(*a) = append((*a)[:i], (*a)[i+1:]...)
			continue // go round and check the data that is now at position i
		}
		i++
	}
}

func orgFromHash(hashStr string) int {
	if hashStr == "" {
		return -1
	}
	pos := strings.Index(hashStr, "/")
	if pos < 0 { // try index table format
		pos = strings.Index(hashStr, ":")
	}
	if pos < 0 { // unrecognized format
		return -1
	}
	org, err := strconv.Atoi(hashStr[:pos])
	if err != nil {
		return -1
	}
	return org
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		fmt.Fprintf(os.Stderr, "%+v", err)
		os.Exit(1)
	}
}
