package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/promql"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		logLevel         util.LogLevel
	)
	util.RegisterFlags(&chunkStoreConfig, &schemaConfig, &storageConfig, &logLevel)
	flag.Parse()

	util.InitLogger(logLevel.AllowedLevel)

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	sampleQueryable := querier.NewQueryable(noopQuerier{}, chunkStore, false)
	engine := promql.NewEngine(sampleQueryable, nil)

	if flag.NArg() != 1 {
		level.Error(util.Logger).Log("usage: oneshot <options> promql-query")
	}

	// Now execute the query
	query, err := engine.NewInstantQuery(flag.Arg(0), time.Now().Add(-time.Hour*24))
	if err != nil {
		level.Error(util.Logger).Log("error in query:", err)
		os.Exit(1)
	}
	ctx := user.InjectOrgID(context.Background(), "2")

	result := query.Exec(ctx)
	fmt.Printf("result: error %s %s\n", result.Err, result.Value)
}

type noopQuerier struct{}

func (n noopQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, nil
}

func (n noopQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (n noopQuerier) LabelValuesForLabelName(context.Context, model.LabelName) (model.LabelValues, error) {
	return nil, nil
}
