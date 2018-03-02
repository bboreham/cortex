package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
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
	checkFatal(err)
	defer chunkStore.Stop()

	if flag.NArg() != 2 {
		level.Error(util.Logger).Log("usage: analyzer <options> timestamp instance")
	}

	ts, err := time.Parse(time.RFC3339, flag.Arg(0))
	checkFatal(err)
	mt := model.TimeFromUnixNano(ts.UnixNano())

	err = chunk.Analyze(chunkStore, schemaConfig, mt, mt, flag.Arg(1))
	checkFatal(err)

	//fmt.Printf("result: error %s %s\n", result.Err, result.Value)
	fmt.Printf("Done\n")
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
}
