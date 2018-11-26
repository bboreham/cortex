package chunk

import (
	"context"
	"flag"
	"sync"

	"github.com/go-kit/kit/log/level"
	"golang.org/x/time/rate"

	"github.com/weaveworks/cortex/pkg/util"
)

type WriterConfig struct {
	writers      int     // Number of writers to run in parallel
	maxQueueSize int     // Max rows to allow in writer queue
	rateLimit    float64 // Max rate to send rows to storage back-end, per writer
	// (In some sense the rate we send should relate to the provisioned capacity in the back-end)
}

type Writer struct {
	WriterConfig

	storage StorageClient

	group   sync.WaitGroup
	pending sync.WaitGroup

	// Clients send items on this chan to be written
	Write chan WriteBatch
	// internally we send items on this chan to be retried
	retry chan WriteBatch
	// Writers read batches of items from this chan
	batched chan WriteBatch
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *WriterConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.writers, "writers", 1, "Number of writers to run in parallel")
	f.IntVar(&cfg.maxQueueSize, "writers-queue-limit", 1000, "Max rows to allow in writer queue")
	f.Float64Var(&cfg.rateLimit, "writer-rate-limit", 1000, "Max rate to send rows to storage back-end, per writer")
}

func NewWriter(cfg WriterConfig, storage StorageClient) *Writer {
	writer := &Writer{
		WriterConfig: cfg,
		storage:      storage,
		// Unbuffered chan so we can tell when batcher has received all items
		Write:   make(chan WriteBatch),
		retry:   make(chan WriteBatch, 100), // we should always accept retry data, to avoid deadlock
		batched: make(chan WriteBatch),
	}
	writer.Run()
	return writer
}

func (c *store) IndexChunk(ctx context.Context, chunk Chunk) error {
	writeReqs, err := c.calculateIndexEntries(chunk.UserID, chunk.From, chunk.Through, chunk)
	if err != nil {
		return err
	}
	c.writer.Write <- writeReqs
	return nil
}

func (writer *Writer) Run() {
	writer.group.Add(1 + writer.writers)
	go func() {
		writer.batcher()
		writer.group.Done()
	}()
	for i := 0; i < writer.writers; i++ {
		go func() {
			writer.writeLoop(context.TODO())
			writer.group.Done()
		}()
	}
}

// Flush all pending items to the backing store
// Note: nothing should be sent to the Write chan until this is over
func (writer *Writer) Flush() {
	// Ensure that batcher has received all items so it won't call Add() any more
	writer.Write <- nil
	// Wait for pending items to be sent to store
	writer.pending.Wait()
}

// Stop all background goroutines after flushing pending writes
func (writer *Writer) Stop() {
	writer.Flush()
	// Close chans to signal writer(s) and batcher to terminate
	close(writer.batched)
	close(writer.retry)
	writer.group.Wait()
}

// writeLoop receives on the 'batched' chan, sends to store, and
// passes anything that was throttled to the 'retry' chan.
func (sc *Writer) writeLoop(ctx context.Context) {
	limiter := rate.NewLimiter(rate.Limit(sc.rateLimit), 100) // burst size should be the largest batch
	for {
		batch, ok := <-sc.batched
		if !ok {
			return
		}
		batchLen := batch.Len()
		level.Debug(util.Logger).Log("msg", "about to write", "num_requests", batchLen)
		retry, err := sc.storage.BatchWriteNoRetry(ctx, batch)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to write; dropping data", "err", err, "batch", batch)
			sc.pending.Add(-batchLen)
			continue
		}
		// Send unprocessed items back into the batcher
		sc.retry <- retry
		sc.pending.Add(-(batchLen - retry.Len()))
		// Wait before accepting the next batch
		limiter.WaitN(ctx, batchLen)
	}
}

// Receive individual requests, and batch them up into groups to send to the store
func (sc *Writer) batcher() {
	flushing := false
	var queue, outBatch WriteBatch
	queue = sc.storage.NewWriteBatch()
	for {
		queueLen := queue.Len()
		writerQueueLength.Set(float64(queueLen)) // Prometheus metric
		var in, out chan WriteBatch
		// We will allow in new data if the queue isn't too long
		if queueLen < sc.maxQueueSize {
			in = sc.Write
		}
		// We will send out a batch if the queue is big enough, or if we're flushing
		if outBatch == nil || outBatch.Len() == 0 {
			outBatch = queue.Take(flushing)
		}
		if outBatch != nil && outBatch.Len() > 0 {
			out = sc.batched
		}
		select {
		case inBatch := <-in:
			if inBatch == nil { // Nil used as interlock to know we received all previous values
				flushing = true
			} else {
				flushing = false
				queue.AddBatch(inBatch)
				sc.pending.Add(inBatch.Len())
			}
		case inBatch, ok := <-sc.retry:
			if !ok {
				return
			}
			queue.AddBatch(inBatch)
		case out <- outBatch:
			outBatch = nil
		}
	}
}
