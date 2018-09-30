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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/logging"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

var (
	pagesPerDot int
)

func main() {
	var (
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		chunkStoreConfig chunk.StoreConfig

		orgsFile string

		week      int64
		segments  int
		tableName string
		loglevel  string
		address   string

		rechunkTablePrefix string
		reindexTablePrefix string
	)

	util.RegisterFlags(&storageConfig, &schemaConfig, &chunkStoreConfig)
	flag.StringVar(&address, "address", ":6060", "Address to listen on, for profiling, etc.")
	flag.Int64Var(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
	flag.StringVar(&orgsFile, "delete-orgs-file", "", "File containing IDs of orgs to delete")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.IntVar(&pagesPerDot, "pages-per-dot", 10, "Print a dot per N pages in DynamoDB (0 to disable)")
	flag.StringVar(&rechunkTablePrefix, "dynamodb.rechunk-prefix", "", "Prefix of new chunk table (blank to disable)")
	flag.StringVar(&reindexTablePrefix, "dynamodb.reindex-prefix", "", "Prefix of new index table (blank to disable)")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	// HTTP listener for profiling
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		checkFatal(http.ListenAndServe(address, nil))
	}()

	orgs := map[int]struct{}{}
	if orgsFile != "" {
		content, err := ioutil.ReadFile(orgsFile)
		checkFatal(err)
		for _, arg := range strings.Fields(string(content)) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			orgs[org] = struct{}{}
		}
	}

	secondsInWeek := int64(7 * 24 * time.Hour.Seconds())
	if week == 0 {
		week = time.Now().Unix() / secondsInWeek
	}

	schemaConfig.EndDate.Set(time.Unix((week+1)*secondsInWeek, 0).Format("2006-01-02"))
	storageOpts, err := storage.Opts(storageConfig, schemaConfig)
	checkFatal(err)
	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageOpts)
	checkFatal(err)
	defer chunkStore.Stop()

	var reindexStore chunk.Store
	if reindexTablePrefix != "" || rechunkTablePrefix != "" {
		reindexSchemaConfig := schemaConfig
		reindexSchemaConfig.ChunkTables.Prefix = rechunkTablePrefix
		reindexSchemaConfig.IndexTables.Prefix = reindexTablePrefix
		reindexOpts, err := storage.Opts(storageConfig, reindexSchemaConfig)
		checkFatal(err)
		reindexStore, err = chunk.NewStore(chunkStoreConfig, reindexSchemaConfig, reindexOpts)
		checkFatal(err)
	}

	tableName = fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, week)
	fmt.Printf("table %s\n", tableName)

	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		handlers[segment] = newHandler(reindexStore, rechunkTablePrefix, reindexTablePrefix, orgs)
		callbacks[segment] = handlers[segment].handlePage
	}

	tableTime := model.TimeFromUnix(week * secondsInWeek)
	err = chunkStore.Scan(context.Background(), tableTime, tableTime, reindexTablePrefix != "", callbacks)
	checkFatal(err)

	totals := newSummary()
	for segment := 0; segment < segments; segment++ {
		totals.accumulate(handlers[segment].summary)
	}
	totals.print()
}

/* TODO: delete v8 schema rows for all instances */

type summary struct {
	counts map[int]int
}

func newSummary() summary {
	return summary{
		counts: map[int]int{},
	}
}

func (s *summary) accumulate(b summary) {
	for k, v := range b.counts {
		s.counts[k] += v
	}
}

func (s summary) print() {
	for user, count := range s.counts {
		fmt.Printf("%d, %d\n", user, count)
	}
}

type handler struct {
	store              chunk.Store
	tableName          string
	pages              int
	orgs               map[int]struct{}
	reindexTablePrefix string
	summary
}

func newHandler(store chunk.Store, tableName string, reindexTablePrefix string, orgs map[int]struct{}) handler {
	return handler{
		store:              store,
		tableName:          tableName,
		orgs:               orgs,
		summary:            newSummary(),
		reindexTablePrefix: reindexTablePrefix,
	}
}

func (h *handler) handlePage(page chunk.ReadBatch) {
	h.pages++
	if pagesPerDot > 0 && h.pages%pagesPerDot == 0 {
		fmt.Printf(".")
	}
	ctx := context.Background()
	decodeContext := chunk.NewDecodeContext()
	for i := page.Iterator(); i.Next(); {
		hashValue := i.HashValue()
		org := chunk.OrgFromHash(hashValue)
		if org <= 0 {
			continue
		}
		h.counts[org]++
		if _, found := h.orgs[org]; found {
			//request := h.storageClient.NewWriteBatch()
			//request.AddDelete(h.tableName, hashValue, page.RangeValue(i))
			//			h.requests <- request
		} else if h.store != nil {
			var ch chunk.Chunk
			err := ch.Decode(decodeContext, i.Value())
			if err != nil {
				level.Error(util.Logger).Log("msg", "chunk decode error", "err", err)
				continue
			}
			if h.tableName == "" { // just write index entries
				err = h.store.IndexChunk(ctx, ch)
				if err != nil {
					level.Error(util.Logger).Log("msg", "indexing error", "err", err)
					continue
				}
			} else {
				err = h.store.Put(ctx, []chunk.Chunk{ch})
				if err != nil {
					level.Error(util.Logger).Log("msg", "put error", "err", err)
					continue
				}
			}
		}
	}
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		os.Exit(1)
	}
}
