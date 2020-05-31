package storage

import (
	"context"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
)

type cacheGenStore struct {
	store             chunk.Store
	cacheGenNumLoader CacheGenNumLoader
}

func newCacheGenStore(store chunk.Store, cacheGenNumLoader CacheGenNumLoader) chunk.Store {
	return cacheGenStore{
		cacheGenNumLoader: cacheGenNumLoader,
		store:             store,
	}
}

// Wrap all methods of chunk.Store

func (c cacheGenStore) Put(ctx context.Context, chunks []Chunk) error {
	return c.store.Put(ctx, chunks)
}

func (c cacheGenStore) PutOne(ctx context.Context, from, through model.Time, chunk Chunk) error {
	return c.store.PutOne(ctx, from, through, chunk)
}

func (c cacheGenStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]Chunk, error) {
	ctx = c.injectCacheGen(ctx, userID)
	return c.store.Get(ctx, userID, from, through, matchers)
}

func (c cacheGenStore) GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([][]Chunk, []*Fetcher, error) {
	ctx = c.injectCacheGen(ctx, userID)
	return c.store.GetChunkRefs(ctx, userID, from, through, matchers)
}

func (c cacheGenStore) LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string) ([]string, error) {
	ctx = c.injectCacheGen(ctx, userID)
	return c.store.LabelValuesForMetricName(ctx, userID, from, through, metricName, labelName)
}

func (c cacheGenStore) LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error) {
	ctx = c.injectCacheGen(ctx, userID)
	return c.store.LabelNamesForMetricName(ctx, userID, from, through, metricName)
}

func (c cacheGenStore) DeleteChunk(ctx context.Context, from, through model.Time, userID, chunkID string, metric labels.Labels, partiallyDeletedInterval *model.Interval) error {
	return c.store.DeleteChunk()
}

func (c cacheGenStore) DeleteSeriesIDs(ctx context.Context, from, through model.Time, userID string, metric labels.Labels) error {
}

func (c cacheGenStore) Stop() {
	c.store.Stop()
}

func (c cacheGenStore) injectCacheGen(ctx context.Context, userID string) context.Context {
	if c.cacheGenNumLoader == nil {
		return ctx
	}

	return cache.InjectCacheGenNumber(ctx, c.cacheGenNumLoader.GetStoreCacheGenNumber(userID))
}
