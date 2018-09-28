package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/user"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		querierConfig    querier.Config
		loglevel         string
	)
	util.RegisterFlags(&chunkStoreConfig, &schemaConfig, &storageConfig, &querierConfig)
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	storageOpts, err := storage.Opts(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageOpts)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	queryable, engine := querier.New(querierConfig, noopDistributor{}, chunkStore)

	if flag.NArg() != 1 {
		level.Error(util.Logger).Log("usage: oneshot <options> promql-query")
	}

	// Now execute the query
	query, err := engine.NewInstantQuery(queryable, flag.Arg(0), time.Now().Add(-time.Hour*24))
	if err != nil {
		level.Error(util.Logger).Log("error in query:", err)
		os.Exit(1)
	}
	ctx := user.InjectOrgID(context.Background(), "2")

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
