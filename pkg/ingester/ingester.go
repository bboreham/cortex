package ingester

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	// Needed for gRPC compatibility.
	old_ctx "golang.org/x/net/context"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"

	"github.com/weaveworks/common/httpgrpc"
	cortex_chunk "github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	ingesterSubsystem  = "ingester"
	discardReasonLabel = "reason"

	// Reasons to discard samples.
	outOfOrderTimestamp     = "timestamp_out_of_order"
	duplicateSample         = "multiple_values_for_timestamp"
	greaterThanMaxSampleAge = "greater_than_max_sample_age"

	// DefaultConcurrentFlush is the number of series to flush concurrently
	DefaultConcurrentFlush = 50
	// DefaultMaxSeriesPerUser is the maximum number of series allowed per user.
	DefaultMaxSeriesPerUser = 5000000
	// DefaultMaxSeriesPerMetric is the maximum number of series in one metric (of a single user).
	DefaultMaxSeriesPerMetric = 50000
)

var (
	memorySeriesDesc = prometheus.NewDesc(
		"cortex_ingester_memory_series",
		"The current number of series in memory.",
		nil, nil,
	)
	memoryUsersDesc = prometheus.NewDesc(
		"cortex_ingester_memory_users",
		"The current number of users in memory.",
		nil, nil,
	)
	flushQueueLengthDesc = prometheus.NewDesc(
		"cortex_ingester_flush_queue_length",
		"The total number of series pending in the flush queue.",
		nil, nil,
	)
)

// Config for an Ingester.
type Config struct {
	RingConfig       ring.Config
	userStatesConfig UserStatesConfig

	// Config for the ingester lifecycle control
	ListenPort       *int
	NumTokens        int
	HeartbeatPeriod  time.Duration
	JoinAfter        time.Duration
	SearchPendingFor time.Duration
	ClaimOnRollout   bool

	// Config for chunk flushing
	FlushCheckPeriod  time.Duration
	MaxChunkIdle      time.Duration
	FlushOpTimeout    time.Duration
	MaxChunkAge       time.Duration
	ConcurrentFlushes int
	ChunkEncoding     string

	// Config for rejecting old samples
	RejectOldSamples       bool
	RejectOldSamplesMaxAge time.Duration

	// For testing, you can override the address and ID of this ingester
	addr                  string
	infName               string
	id                    string
	skipUnregister        bool
	ingesterClientFactory func(addr string, withCompression bool) (client.IngesterClient, error)
	KVClient              ring.KVClient
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RingConfig.RegisterFlags(f)
	cfg.userStatesConfig.RegisterFlags(f)

	f.IntVar(&cfg.NumTokens, "ingester.num-tokens", 128, "Number of tokens for each ingester.")
	f.DurationVar(&cfg.HeartbeatPeriod, "ingester.heartbeat-period", 5*time.Second, "Period at which to heartbeat to consul.")
	f.DurationVar(&cfg.JoinAfter, "ingester.join-after", 0*time.Second, "Period to wait for a claim from another ingester; will join automatically after this.")
	f.DurationVar(&cfg.SearchPendingFor, "ingester.search-pending-for", 30*time.Second, "Time to spend searching for a pending ingester when shutting down.")
	f.BoolVar(&cfg.ClaimOnRollout, "ingester.claim-on-rollout", false, "Send chunks to PENDING ingesters on exit.")

	f.DurationVar(&cfg.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	f.DurationVar(&cfg.FlushOpTimeout, "ingester.flush-op-timeout", 1*time.Minute, "Timeout for individual flush operations.")
	f.DurationVar(&cfg.MaxChunkIdle, "ingester.max-chunk-idle", 5*time.Minute, "Maximum chunk idle time before flushing.")
	f.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age before flushing.")
	f.IntVar(&cfg.ConcurrentFlushes, "ingester.concurrent-flushes", DefaultConcurrentFlush, "Number of concurrent goroutines flushing to dynamodb.")
	f.StringVar(&cfg.ChunkEncoding, "ingester.chunk-encoding", "1", "Encoding version to use for chunks.")

	f.BoolVar(&cfg.RejectOldSamples, "ingester.reject-old-samples", false, "Reject old samples.")
	f.DurationVar(&cfg.RejectOldSamplesMaxAge, "ingester.reject-old-samples.max-age", 14*24*time.Hour, "Maximum accepted sample age before rejecting.")

	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	f.StringVar(&cfg.infName, "ingester.interface", "eth0", "Name of network interface to read address from.")
	f.StringVar(&cfg.addr, "ingester.addr", "", "IP address to register into consul.")
	f.StringVar(&cfg.id, "ingester.id", hostname, "ID to register into consul.")
}

// Ingester deals with "in flight" chunks.
// Its like MemorySeriesStorage, but simpler.
type Ingester struct {
	cfg         Config
	chunkStore  ChunkStore
	ringKVStore ring.KVClient

	userStatesMtx sync.RWMutex
	userStates    *userStates

	// These values are initialised at startup, and never change
	id   string
	addr string

	// Controls the lifecycle of the ingester
	stopLock  sync.RWMutex
	stopped   bool
	quit      chan struct{}
	done      sync.WaitGroup
	actorChan chan func()

	// We need to remember the ingester state just in case consul goes away and comes
	// back empty.  And it changes during lifecycle of ingester.
	state  ring.IngesterState
	tokens []uint32

	// Controls the ready-reporting
	readyLock sync.Mutex
	startTime time.Time
	ready     bool

	// One queue per flush thread.  Fingerprint is used to
	// pick a queue.
	flushQueues     []*util.PriorityQueue
	flushQueuesDone sync.WaitGroup

	// Hook for injecting behaviour from tests.
	preFlushUserSeries func()

	ingestedSamples  prometheus.Counter
	chunkUtilization prometheus.Histogram
	chunkLength      prometheus.Histogram
	chunkAge         prometheus.Histogram
	queries          prometheus.Counter
	queriedSamples   prometheus.Counter
	memoryChunks     prometheus.Gauge
}

// ChunkStore is the interface we need to store chunks
type ChunkStore interface {
	Put(ctx context.Context, chunks []cortex_chunk.Chunk) error
}

// New constructs a new Ingester.
func New(cfg Config, chunkStore ChunkStore) (*Ingester, error) {
	if cfg.FlushCheckPeriod == 0 {
		cfg.FlushCheckPeriod = 1 * time.Minute
	}
	if cfg.MaxChunkIdle == 0 {
		cfg.MaxChunkIdle = 1 * time.Hour
	}
	if cfg.ConcurrentFlushes <= 0 {
		cfg.ConcurrentFlushes = DefaultConcurrentFlush
	}
	if cfg.ChunkEncoding == "" {
		cfg.ChunkEncoding = "1"
	}
	if cfg.userStatesConfig.RateUpdatePeriod == 0 {
		cfg.userStatesConfig.RateUpdatePeriod = 15 * time.Second
	}
	if cfg.userStatesConfig.MaxSeriesPerUser <= 0 {
		cfg.userStatesConfig.MaxSeriesPerUser = DefaultMaxSeriesPerUser
	}
	if cfg.userStatesConfig.MaxSeriesPerMetric <= 0 {
		cfg.userStatesConfig.MaxSeriesPerMetric = DefaultMaxSeriesPerMetric
	}
	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = client.MakeIngesterClient
	}

	if err := chunk.DefaultEncoding.Set(cfg.ChunkEncoding); err != nil {
		return nil, err
	}

	kvstore := cfg.KVClient
	if kvstore == nil {
		var err error
		codec := ring.ProtoCodec{Factory: ring.ProtoDescFactory}
		kvstore, err = ring.NewConsulClient(cfg.RingConfig.ConsulConfig, codec)
		if err != nil {
			return nil, err
		}
	}

	addr := cfg.addr
	if addr == "" {
		var err error
		addr, err = util.GetFirstAddressOf(cfg.infName)
		if err != nil {
			return nil, err
		}
	}

	i := &Ingester{
		cfg:         cfg,
		ringKVStore: kvstore,
		chunkStore:  chunkStore,
		userStates:  newUserStates(&cfg.userStatesConfig),

		addr: fmt.Sprintf("%s:%d", addr, *cfg.ListenPort),
		id:   cfg.id,

		quit:      make(chan struct{}),
		actorChan: make(chan func()),
		state:     ring.PENDING,
		startTime: time.Now(),

		flushQueues: make([]*util.PriorityQueue, cfg.ConcurrentFlushes, cfg.ConcurrentFlushes),

		ingestedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_total",
			Help: "The total number of samples ingested.",
		}),
		chunkUtilization: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_utilization",
			Help:    "Distribution of stored chunk utilization (when stored).",
			Buckets: prometheus.LinearBuckets(0, 0.2, 6),
		}),
		chunkLength: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_length",
			Help:    "Distribution of stored chunk lengths (when stored).",
			Buckets: prometheus.ExponentialBuckets(10, 2, 10), // biggest bucket is 10*2^(10-1) = 5120
		}),
		chunkAge: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_age_seconds",
			Help:    "Distribution of chunk ages (when stored).",
			Buckets: prometheus.ExponentialBuckets(60, 2, 11), // biggest bucket is 60*2^(11-1) = 61440 = 17:04 hrs
		}),
		memoryChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_chunks",
			Help: "The total number of chunks in memory.",
		}),
		queries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_queries_total",
			Help: "The total number of queries the ingester has handled.",
		}),
		queriedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_queried_samples_total",
			Help: "The total number of samples returned from queries.",
		}),
	}

	i.flushQueuesDone.Add(cfg.ConcurrentFlushes)

	for j := 0; j < cfg.ConcurrentFlushes; j++ {
		i.flushQueues[j] = util.NewPriorityQueue()
		go i.flushLoop(j)
	}

	i.done.Add(1)
	go i.loop()

	return i, nil
}

// Push implements client.IngesterServer
func (i *Ingester) Push(ctx old_ctx.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	var lastPartialErr error
	samples := util.FromWriteRequest(req)

samples:
	for j := range samples {
		if err := i.append(ctx, &samples[j]); err != nil {
			if httpResp, ok := httpgrpc.HTTPResponseFromError(err); ok {
				switch httpResp.Code {
				case http.StatusBadRequest, http.StatusTooManyRequests:
					lastPartialErr = err
					continue samples
				}
			}
			return nil, err
		}
	}

	return &client.WriteResponse{}, lastPartialErr
}

func (i *Ingester) append(ctx context.Context, sample *model.Sample) error {
	if i.cfg.RejectOldSamples && sample.Timestamp < model.Now().Add(-i.cfg.RejectOldSamplesMaxAge) {
		discardedSamples.WithLabelValues(greaterThanMaxSampleAge).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, "sample with timestamp %v is older than the maximum accepted age", sample.Timestamp)
	}

	if err := util.ValidateSample(sample); err != nil {
		level.Error(util.WithContext(ctx, util.Logger)).Log("msg", "error validating sample", "err", err)
		return nil
	}

	for ln, lv := range sample.Metric {
		if len(lv) == 0 {
			delete(sample.Metric, ln)
		}
	}

	i.stopLock.RLock()
	defer i.stopLock.RUnlock()
	if i.stopped {
		return fmt.Errorf("ingester stopping")
	}

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, fp, series, err := i.userStates.getOrCreateSeries(ctx, sample.Metric)
	if err != nil {
		return err
	}
	defer func() {
		state.fpLocker.Unlock(fp)
	}()

	prevNumChunks := len(series.chunkDescs)
	if err := series.add(model.SamplePair{
		Value:     sample.Value,
		Timestamp: sample.Timestamp,
	}); err != nil {
		return err
	}

	i.memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
	i.ingestedSamples.Inc()
	state.ingestedSamples.inc()

	return err
}

// Query implements service.IngesterServer
func (i *Ingester) Query(ctx old_ctx.Context, req *client.QueryRequest) (*client.QueryResponse, error) {
	start, end, matchers, err := util.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	matrix, err := i.query(ctx, start, end, matchers)
	if err != nil {
		return nil, err
	}

	return util.ToQueryResponse(matrix), nil
}

func (i *Ingester) query(ctx context.Context, from, through model.Time, matchers []*labels.Matcher) (model.Matrix, error) {
	i.queries.Inc()

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, err := i.userStates.getOrCreate(ctx)
	if err != nil {
		return nil, err
	}

	queriedSamples := 0
	result := model.Matrix{}
	err = state.forSeriesMatching(matchers, func(_ model.Fingerprint, series *memorySeries) error {
		values, err := series.samplesForRange(from, through)
		if err != nil {
			return err
		}

		result = append(result, &model.SampleStream{
			Metric: series.metric,
			Values: values,
		})
		queriedSamples += len(values)
		return nil
	})
	i.queriedSamples.Add(float64(queriedSamples))
	return result, err
}

// LabelValues returns all label values that are associated with a given label name.
func (i *Ingester) LabelValues(ctx old_ctx.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, err := i.userStates.getOrCreate(ctx)
	if err != nil {
		return nil, err
	}

	resp := &client.LabelValuesResponse{}
	for _, v := range state.index.lookupLabelValues(model.LabelName(req.LabelName)) {
		resp.LabelValues = append(resp.LabelValues, string(v))
	}

	return resp, nil
}

// MetricsForLabelMatchers returns all the metrics which match a set of matchers.
func (i *Ingester) MetricsForLabelMatchers(ctx old_ctx.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, err := i.userStates.getOrCreate(ctx)
	if err != nil {
		return nil, err
	}

	// TODO Right now we ignore start and end.
	_, _, matchersSet, err := util.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	metrics := map[model.Fingerprint]model.Metric{}
	for _, matchers := range matchersSet {
		if err := state.forSeriesMatching(matchers, func(fp model.Fingerprint, series *memorySeries) error {
			if _, ok := metrics[fp]; !ok {
				metrics[fp] = series.metric
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	result := []model.Metric{}
	for _, metric := range metrics {
		result = append(result, metric)
	}

	return util.ToMetricsForLabelMatchersResponse(result), nil
}

// UserStats returns ingestion statistics for the current user.
func (i *Ingester) UserStats(ctx old_ctx.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, err := i.userStates.getOrCreate(ctx)
	if err != nil {
		return nil, err
	}

	return &client.UserStatsResponse{
		IngestionRate: state.ingestedSamples.rate(),
		NumSeries:     uint64(state.fpToSeries.length()),
	}, nil
}

// Describe implements prometheus.Collector.
func (i *Ingester) Describe(ch chan<- *prometheus.Desc) {
	ch <- memorySeriesDesc
	ch <- memoryUsersDesc
	ch <- flushQueueLengthDesc
	ch <- i.ingestedSamples.Desc()
	ch <- i.chunkUtilization.Desc()
	ch <- i.chunkLength.Desc()
	ch <- i.chunkAge.Desc()
	ch <- i.queries.Desc()
	ch <- i.queriedSamples.Desc()
	ch <- i.memoryChunks.Desc()
}

// Collect implements prometheus.Collector.
func (i *Ingester) Collect(ch chan<- prometheus.Metric) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	numUsers := i.userStates.numUsers()
	numSeries := i.userStates.numSeries()

	ch <- prometheus.MustNewConstMetric(
		memorySeriesDesc,
		prometheus.GaugeValue,
		float64(numSeries),
	)
	ch <- prometheus.MustNewConstMetric(
		memoryUsersDesc,
		prometheus.GaugeValue,
		float64(numUsers),
	)

	flushQueueLength := 0
	for _, flushQueue := range i.flushQueues {
		flushQueueLength += flushQueue.Length()
	}
	ch <- prometheus.MustNewConstMetric(
		flushQueueLengthDesc,
		prometheus.GaugeValue,
		float64(flushQueueLength),
	)
	ch <- i.ingestedSamples
	ch <- i.chunkUtilization
	ch <- i.chunkLength
	ch <- i.chunkAge
	ch <- i.queries
	ch <- i.queriedSamples
	ch <- i.memoryChunks
}
