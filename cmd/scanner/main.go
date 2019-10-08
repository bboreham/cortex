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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
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

	reEncodeChunks bool
)

func main() {
	var (
		schemaConfig     chunk.SchemaConfig
		rechunkConfig    chunk.SchemaConfig
		storageConfig    storage.Config
		chunkStoreConfig chunk.StoreConfig
		tbmConfig        chunk.TableManagerConfig

		deleteOrgsFile string
		includeOrgsStr string

		week      int64
		segments  int
		tableName string
		loglevel  string
		address   string

		rechunkSchemaFile string
	)

	flagext.RegisterFlags(&storageConfig, &schemaConfig, &chunkStoreConfig, &tbmConfig)
	flag.StringVar(&address, "address", ":6060", "Address to listen on, for profiling, etc.")
	flag.Int64Var(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
	flag.StringVar(&deleteOrgsFile, "delete-orgs-file", "", "File containing IDs of orgs to delete")
	flag.StringVar(&includeOrgsStr, "include-orgs", "", "IDs of orgs to include (space-separated)")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.StringVar(&rechunkSchemaFile, "rechunk-yaml", "", "Yaml definition of new chunk tables (blank to disable)")
	flag.BoolVar(&reEncodeChunks, "re-encode-chunks", false, "Enable re-encoding of chunks to save on storing zeros")

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

	err := schemaConfig.Load()
	checkFatal(err)

	overrides := &validation.Overrides{}
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	checkFatal(err)
	defer chunkStore.Stop()

	var reindexStore chunk.Store
	if rechunkSchemaFile != "" {
		err := rechunkConfig.LoadFromFile(rechunkSchemaFile)
		checkFatal(err)
		if len(rechunkConfig.Configs) != 1 {
			checkFatal(fmt.Errorf("rechunk config must have 1 config"))
		}
		reindexStore, err = storage.NewStore(storageConfig, chunkStoreConfig, rechunkConfig, overrides)
		checkFatal(err)

		// We need to "trick" the metrics auto-scaling into scaling up and down,
		// even though we don't have a queue that it's used to looking at.
		// Pretend we have a queue that is always enormous and also always shrinking
		trickQuery := "2000000000-timestamp(count(up))"
		storageConfig.AWSStorageConfig.Metrics.QueueLengthQuery = trickQuery

		tableClient, err := storage.NewTableClient(rechunkConfig.Configs[0].IndexType, storageConfig)
		util.CheckFatal("initializing table client", err)

		// We want our table-manager to manage just a two-week period
		rechunkConfig.Configs[0].From.Time = tableTime
		tbmConfig.CreationGracePeriod = time.Hour * 169
		tableManager, err := chunk.NewTableManager(tbmConfig, rechunkConfig, 0, tableClient, nil)
		util.CheckFatal("initializing table manager", err)
		err = tableManager.SyncTables(context.Background(), tableTime.Time())
		util.CheckFatal("sync tables", err)
		time.Sleep(time.Minute) // allow time for tables to be created.  FIXME do this better
		// Sync continuously in background
		go tmLoop(context.Background(), tableManager, tableTime.Time())
	}

	tableName, err = schemaConfig.ChunkTableFor(tableTime)
	checkFatal(err)
	fmt.Printf("table %s\n", tableName)

	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		handlers[segment] = newHandler(reindexStore, tableName, includeOrgs, deleteOrgs)
		callbacks[segment] = handlers[segment].handlePage
	}

	err = chunkStore.(chunk.Store2).Scan(context.Background(), tableTime, tableTime, rechunkSchemaFile != "", callbacks)
	checkFatal(err)

	if reindexStore != nil {
		reindexStore.Stop()
	}

	totals := newSummary()
	for segment := 0; segment < segments; segment++ {
		totals.accumulate(handlers[segment].summary)
	}
	totals.print(deleteOrgs)
}

// Sync like the real table-manager does, but at the specific time we are operating
func tmLoop(ctx context.Context, m *chunk.TableManager, atTime time.Time) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.SyncTables(ctx, atTime); err != nil {
				level.Error(util.Logger).Log("msg", "error syncing tables", "err", err)
			}
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
	store       chunk.Store
	tableName   string
	pages       int
	includeOrgs map[int]struct{}
	deleteOrgs  map[int]struct{}
	summary
}

func newHandler(store chunk.Store, tableName string, includeOrgs map[int]struct{}, deleteOrgs map[int]struct{}) handler {
	if len(includeOrgs) == 0 {
		includeOrgs = nil
	}
	return handler{
		store:       store,
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
	pageCounter.WithLabelValues(h.tableName).Inc()
	decodeContext := chunk.NewDecodeContext()
	for i := page.Iterator().(ReadBatchHashIterator); i.Next(); {
		hashValue := i.HashValue()
		org := orgFromHash(hashValue)
		if org <= 0 {
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
			//request := h.storageClient.NewWriteBatch()
			//request.AddDelete(h.tableName, hashValue, i.RangeValue())
			//			h.requests <- request
		} else if h.store != nil {
			var ch chunk.Chunk
			err := ch.Decode(decodeContext, i.Value())
			if err != nil {
				level.Error(util.Logger).Log("msg", "chunk decode error", "err", err)
				continue
			}
			h.counts[org][string(ch.Metric.Get(model.MetricNameLabel))]++
			{
				if reEncodeChunks {
					err = ch.Encode()
					if err != nil {
						level.Error(util.Logger).Log("msg", "re-encode error", "err", err)
						continue
					}
				}
				err = h.store.Put(ctx, []chunk.Chunk{ch})
				if err != nil {
					level.Error(util.Logger).Log("msg", "put error", "err", err)
					continue
				}
			}
		}
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
		os.Exit(1)
	}
}
