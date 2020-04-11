package chunk

import "context"
import "github.com/prometheus/common/model"

// IndexClient is a client for the storage of the index (e.g. DynamoDB or Bigtable).
type IndexClient interface {
	Stop()

	// For the write path.
	NewWriteBatch() WriteBatch
	BatchWrite(context.Context, WriteBatch) error

	// For the read path.
	QueryPages(ctx context.Context, queries []IndexQuery, callback func(IndexQuery, ReadBatch) (shouldContinue bool)) error
}

// ObjectClient is for storing and retrieving chunks.
type ObjectClient interface {
	Stop()

	PutChunks(ctx context.Context, chunks []Chunk) error
	GetChunks(ctx context.Context, chunks []Chunk) ([]Chunk, error)
}

// ObjectAndIndexClient allows optimisations where the same client handles both
type ObjectAndIndexClient interface {
	PutChunkAndIndex(ctx context.Context, c Chunk, index WriteBatch) error
}

// ObjectClient2 because naming is hard
type ObjectClient2 interface {
	ObjectClient
	Scan(ctx context.Context, from, through model.Time, withValue bool, startSegment, totalSegments int, callbacks []func(result ReadBatch)) error
}

// WriteBatch represents a batch of writes.
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte, value []byte)
}

// ReadBatch represents the results of a QueryPages.
type ReadBatch interface {
	Iterator() ReadBatchIterator
}

// ReadBatchIterator is an iterator over a ReadBatch.
type ReadBatchIterator interface {
	Next() bool
	RangeValue() []byte
	Value() []byte
}
