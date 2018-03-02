package chunk

import (
	"fmt"

	"github.com/prometheus/common/model"
)

func Analyze(s *Store, c SchemaConfig, from, through model.Time, userID string) error {
	fmt.Printf("Start\n")

	for _, bucket := range c.dailyBuckets(from, through, userID) {
		err := s.storage.AnalyzeBucket(bucket.tableName, bucket.hashKey)
		if err != nil {
			return err
		}
	}
	return nil
}
