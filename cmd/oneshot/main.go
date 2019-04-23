package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	ot "github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/tracing"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
)

func main() {
	var (
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		querierConfig    querier.Config
		loglevel         string
		endTime          string
		queryParallelism int
	)
	util.RegisterFlags(&chunkStoreConfig, &schemaConfig, &storageConfig, &querierConfig)
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.StringVar(&endTime, "end-time", "", "Time of query in RFC3339 format; default to now")
	flag.IntVar(&queryParallelism, "querier.query-parallelism", 100, "Max subqueries run in parallel per higher-level query.")
	flag.Parse()
	chunk_util.QueryParallelism = queryParallelism

	trace := tracing.NewFromEnv("oneshot")
	defer trace.Close()
	sp, ctx := ot.StartSpanFromContext(context.Background(), "oneshot")
	defer sp.Finish()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	queryable, engine := querier.New(querierConfig, noopDistributor{}, chunkStore)

	if flag.NArg() != 1 {
		level.Error(util.Logger).Log("usage: oneshot <options> promql-query")
	}

	end := time.Now().Add(-time.Hour)
	if endTime != "" {
		end, err = time.Parse(time.RFC3339, endTime)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error in -end-time", "err", err)
			os.Exit(1)
		}
	}

	// Now execute the query
	query, err := engine.NewInstantQuery(queryable, flag.Arg(0), end)
	if err != nil {
		level.Error(util.Logger).Log("error in query:", err)
		os.Exit(1)
	}
	ctx = user.InjectOrgID(ctx, "2")

	result := query.Exec(ctx)
	fmt.Printf("result: error %s %s\n", result.Err, result.Value)
}

type noopDistributor struct{}

func (n noopDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, nil
}

func (n noopDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (n noopDistributor) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, nil
}

func (n noopDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]client.TimeSeriesChunk, error) {
	return nil, nil
}
