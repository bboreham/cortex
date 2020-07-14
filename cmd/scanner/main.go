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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	promStorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
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
		tsdbConfig       cortex_tsdb.Config
		storageConfig    storage.Config
		chunkStoreConfig chunk.StoreConfig
		encodingConfig   encoding.Config
		tbmConfig        chunk.TableManagerConfig
		limitsConfig     validation.Limits

		deleteOrgsFile string
		includeOrgsStr string

		week     int64
		segments int
		loglevel string
		address  string

		chunkReadCapacity int64
		indexReadCapacity int64
		rechunkSchemaFile string
	)

	flagext.RegisterFlags(&storageConfig, &schemaConfig, &chunkStoreConfig, &encodingConfig, &tbmConfig, &limitsConfig)
	flagext.RegisterFlags(&tsdbConfig)
	flag.StringVar(&address, "address", ":6060", "Address to listen on, for profiling, etc.")
	flag.Int64Var(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.Int64Var(&chunkReadCapacity, "chunk-read-provision", 1000, "DynamoDB read provision for chunk table")
	flag.Int64Var(&indexReadCapacity, "index-read-provision", 1000, "DynamoDB read provision for chunk table")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
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

	overrides, err := validation.NewOverrides(limitsConfig, nil)
	checkFatal(err)
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides, prometheus.DefaultRegisterer, nil)
	checkFatal(err)
	defer chunkStore.Stop()

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

	scanner, err := newScanner(tsdbConfig, prometheus.DefaultRegisterer)
	checkFatal(err)
	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		// This only works for series-store, i.e. schema v9 and v10
		handlers[segment] = newHandler(chunkStore.(chunk.Store2), scanner, indexTableName, includeOrgs, deleteOrgs)
		callbacks[segment] = handlers[segment].handlePage
	}

	type tableScanner interface {
		Scan(ctx context.Context, tableName string, from, through model.Time, withValue bool, callbacks []func(result chunk.ReadBatch)) error
	}
	err = readClient.(tableScanner).Scan(context.Background(), indexTableName, tableTime, tableTime, false, callbacks)
	checkFatal(err)

	level.Info(util.Logger).Log("msg", "finished, shutting down")
	// TODO: ship blocks to S3

	// Set back as it was before
	_, err = setReadCapacity(context.Background(), readClient, tableName, prevReadCapacity)
	checkFatal(err)
	_, err = setReadCapacity(context.Background(), readClient, indexTableName, prevIndexReadCapacity)
	checkFatal(err)

	totals := newSummary()
	for segment := 0; segment < segments; segment++ {
		totals.accumulate(handlers[segment].summary)
	}
	totals.print(deleteOrgs)

	time.Sleep(20 * time.Second) // get one more scrape in for final metrics
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

type Shipper interface {
	Sync(ctx context.Context) (uploaded int, err error)
}

type userTSDB struct {
	*tsdb.DB
}

// TSDBState holds data structures used by the TSDB storage engine
type TSDBState struct {
	dbs    map[string]*userTSDB // tsdb sharded by userID
	bucket objstore.Bucket

	// Keeps count of in-flight requests
	inflightWriteReqs sync.WaitGroup

	// Head compactions metrics.
	compactionsTriggered   prometheus.Counter
	compactionsFailed      prometheus.Counter
	walReplayTime          prometheus.Histogram
	appenderAddDuration    prometheus.Histogram
	appenderCommitDuration prometheus.Histogram
}

type scanner struct {
	sync.RWMutex

	cfg struct {
		TSDBConfig cortex_tsdb.Config
	}
	TSDBState
}

func newScanner(cfg cortex_tsdb.Config, registerer prometheus.Registerer) (*scanner, error) {
	bucketClient, err := cortex_tsdb.NewBucketClient(context.Background(), cfg, "scanner", util.Logger, registerer)
	if err != nil {
		return nil, err
	}
	return &scanner{
		cfg: struct{ TSDBConfig cortex_tsdb.Config }{
			TSDBConfig: cfg,
		},
		TSDBState: TSDBState{
			dbs:    make(map[string]*userTSDB),
			bucket: bucketClient,

			compactionsTriggered: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Name: "cortex_ingester_tsdb_compactions_triggered_total",
				Help: "Total number of triggered compactions.",
			}),

			compactionsFailed: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Name: "cortex_ingester_tsdb_compactions_failed_total",
				Help: "Total number of compactions that failed.",
			}),
			walReplayTime: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
				Name:    "cortex_ingester_tsdb_wal_replay_duration_seconds",
				Help:    "The total time it takes to open and replay a TSDB WAL.",
				Buckets: prometheus.DefBuckets,
			}),
			appenderAddDuration: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
				Name:    "cortex_ingester_tsdb_appender_add_duration_seconds",
				Help:    "The total time it takes for a push request to add samples to the TSDB appender.",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}),
			appenderCommitDuration: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
				Name:    "cortex_ingester_tsdb_appender_commit_duration_seconds",
				Help:    "The total time it takes for a push request to commit samples appended to TSDB.",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}),
		},
	}, nil
}

func (i *scanner) getOrCreateTSDB(userID string) (*userTSDB, error) {
	db := i.getTSDB(userID)
	if db != nil {
		return db, nil
	}

	i.Lock()
	defer i.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = i.TSDBState.dbs[userID]
	if ok {
		return db, nil
	}

	// Create the database and a shipper for a user
	db, err := i.createTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Add the db to list of user databases
	i.TSDBState.dbs[userID] = db

	return db, nil
}

func (i *scanner) getTSDB(userID string) *userTSDB {
	i.RLock()
	defer i.RUnlock()
	db := i.TSDBState.dbs[userID]
	return db
}

// createTSDB creates a TSDB for a given userID, and returns the created db.
func (i *scanner) createTSDB(userID string) (*userTSDB, error) {
	tsdbPromReg := prometheus.NewRegistry()
	udir := i.cfg.TSDBConfig.BlocksDir(userID)
	userLogger := util.WithUserID(userID, util.Logger)

	blockRanges := i.cfg.TSDBConfig.BlockRanges.ToMilliseconds()

	// Create a new user database
	db, err := tsdb.Open(udir, userLogger, tsdbPromReg, &tsdb.Options{
		RetentionDuration: i.cfg.TSDBConfig.Retention.Milliseconds(),
		MinBlockDuration:  blockRanges[0],
		MaxBlockDuration:  blockRanges[len(blockRanges)-1],
		NoLockfile:        true,
		StripeSize:        i.cfg.TSDBConfig.StripeSize,
		WALCompression:    i.cfg.TSDBConfig.WALCompressionEnabled,
	})
	if err != nil {
		return nil, err
	}
	db.DisableCompactions() // we will compact on our own schedule

	userDB := &userTSDB{
		DB: db,
	}

	return userDB, nil
}

func (i *scanner) closeAllTSDB() {
	i.Lock()

	wg := &sync.WaitGroup{}
	wg.Add(len(i.TSDBState.dbs))

	// Concurrently close all users TSDB
	for userID, userDB := range i.TSDBState.dbs {
		userID := userID

		go func(db *userTSDB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(util.Logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones. This lock acquisition doesn't deadlock with the
			// outer one, because the outer one is released as soon as all go
			// routines are started.
			i.Lock()
			delete(i.TSDBState.dbs, userID)
			i.Unlock()
		}(userDB)
	}

	// Wait until all Close() completed
	i.Unlock()
	wg.Wait()
}

type handler struct {
	readStore   chunk.Store2
	scanner     *scanner
	tableName   string
	pages       int
	includeOrgs map[int]struct{}
	deleteOrgs  map[int]struct{}
	summary
}

func newHandler(readStore chunk.Store2, scanner *scanner, tableName string, includeOrgs map[int]struct{}, deleteOrgs map[int]struct{}) handler {
	if len(includeOrgs) == 0 {
		includeOrgs = nil
	}
	return handler{
		readStore:   readStore,
		scanner:     scanner,
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
		if h.readStore.DoneThisSeriesBefore(ctx, from, through, orgStr, seriesID) {
			continue
		}

		/*
			ideas:
			 fetch a whole week's data at a time for one timeseries
			 split blocks by size, e.g. when it reaches 2GB, start a new block and ship the old

		*/

		chunks, err := fetchChunks(h.readStore, orgStr, seriesID, from, through)
		if err != nil {
			level.Error(util.Logger).Log("msg", "chunk fetch error", "err", err)
			continue
		}
		newChunk := chunks[0]
		metricName := newChunk.Metric.Get(labels.MetricName)
		switch {
		case isBogus(org, newChunk.Metric):
			h.counts[org][metricName+"-bogus"]++
		case newChunk.Data.Len() < minChunkLength:
			h.counts[org][metricName+"-bogus-tiny"]++
		default:
			// Check again in case another thread completed this one while we were reading
			if h.readStore.DoneThisSeriesBefore(ctx, from, through, orgStr, seriesID) {
				continue
			}
			// TODO: run a query on TSDB to see if we have this series already
			userDB, err := h.scanner.getOrCreateTSDB(orgStr)
			checkFatal(err)
			err = dedupeAndStore(chunks, userDB.DB, from, through)
			if err != nil {
				level.Error(util.Logger).Log("msg", "chunk store error", "err", err)
				continue
			}
			chunksPerUser.WithLabelValues(orgStr).Inc()
			h.counts[org][metricName]++
		}
		// This cache write may duplicate what the store did, but we can't
		// guarantee it's v9+, and don't know we have the same series IDs as it has
		h.readStore.MarkThisSeriesDone(context.TODO(), from, through, orgStr, seriesID)
	}
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

func fetchChunks(dstore chunk.Store2, userID, seriesID string, from, through model.Time) ([]chunk.Chunk, error) {
	// FIXME: this shouldn't really be necessary, but store query APIs rely on it
	ctx := user.InjectOrgID(context.Background(), userID)
	chunks, err := dstore.AllChunksForSeries(ctx, userID, seriesID, from, through)
	if err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks found for %s:%s for %v-%v", userID, seriesID, from, through)
	}
	return chunks, nil
}

// unpack and dedupe all samples from previous chunks; store in TSDB
func dedupeAndStore(chunks []chunk.Chunk, db *tsdb.DB, from, through model.Time) error {
	app := db.Appender()

	iter := batch.NewChunkMergeIterator(chunks, 0, 0)
	if !iter.Seek(int64(from)) {
		return fmt.Errorf("Could not seek to start time")
	}
	labels := chunks[0].Metric
	var ref uint64
	var err error
	for {
		ts, v := iter.At()
		if ts > int64(through) {
			break
		}
		if ref != 0 {
			// Try using reference from previous Add()
			err = app.AddFast(ref, ts, v)
			if err == nil {
				goto next
			} else if errors.Cause(err) != promStorage.ErrNotFound {
				return errors.Wrapf(err, "adding %v %v", labels, ts)
			}
		}
		// Either we didn't have a reference, or we had one and it was rejected
		ref, err = app.Add(labels, ts, v)
		if err != nil {
			return errors.Wrapf(err, "adding %v %v", labels, ts)
		}
	next:
		if !iter.Next() {
			break
		}
	}
	return nil
}

func isBogus(org int, lbls labels.Labels) bool {
	if !removeBogusKubeletMetrics {
		return false
	}
	metricName := lbls.Get(labels.MetricName)
	if org == 13040 || org == 13049 || org == 13124 {
		if strings.HasPrefix(metricName, "kafka_streams_stream_processor_node_metrics_") ||
			strings.HasPrefix(metricName, "kafka_consumer_consumer_node_metrics_") ||
			strings.HasPrefix(metricName, "kafka_producer_producer_node_metrics_") {
			return true
		}
	}
	if strings.HasPrefix(metricName, "container_") {
		// Drop metrics which are disabled but still sent as all zeros by kubelet
		if metricName == "container_network_tcp_usage_total" ||
			metricName == "container_network_udp_usage_total" ||
			metricName == "container_tasks_state" ||
			metricName == "container_cpu_load_average_10s" {
			return true
		}
		// Drop all cAdvisor metrics for leaked systemd tasks
		id := lbls.Get("id")
		if strings.HasPrefix(id, "/system.slice/run-") && strings.HasSuffix(id, "scope") {
			return true
		}
	}
	if metricName == "kube_configmap_metadata_resource_version" {
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
