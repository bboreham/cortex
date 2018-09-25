package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/go-kit/kit/log/level"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/logging"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

type scanner struct {
	week            int
	segments        int
	deleteBatchSize int
	tableName       string

	dynamoDB *dynamodb.DynamoDB
}

var (
	pagesPerDot int
)

func main() {
	var (
		schemaConfig  chunk.SchemaConfig
		storageConfig storage.Config

		scanner scanner
	)

	util.RegisterFlags(&storageConfig, &schemaConfig)
	flag.IntVar(&scanner.week, "week", 0, "Week number to scan, e.g. 2497")
	flag.IntVar(&scanner.segments, "segments", 1, "Number of segments to run in parallel")
	flag.IntVar(&scanner.deleteBatchSize, "delete-batch-size", 10, "Number of delete requests to batch up")
	flag.IntVar(&pagesPerDot, "pages-per-dot", 10, "Print a dot per N pages in DynamoDB (0 to disable)")

	flag.Parse()

	var l logging.Level
	l.Set("debug")
	util.Logger, _ = util.NewPrometheusLogger(l)

	if scanner.week == 0 {
		scanner.week = int(time.Now().Unix() / int64(7*24*time.Hour/time.Second))
	}

	config, err := awscommon.ConfigFromURL(storageConfig.AWSStorageConfig.DynamoDB.URL)
	checkFatal(err)
	session := session.New(config)
	scanner.dynamoDB = dynamodb.New(session)

	var group sync.WaitGroup
	group.Add(scanner.segments)
	totals := newSummary()
	var totalsMutex sync.Mutex

	scanner.tableName = fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, scanner.week)
	fmt.Printf("table %s\n", scanner.tableName)

	deleteChan := make(chan map[string]*dynamodb.AttributeValue, 100)
	var deleteGroup sync.WaitGroup
	deleteGroup.Add(1)
	go scanner.deleteLoop(deleteChan, &deleteGroup)

	for segment := 0; segment < scanner.segments; segment++ {
		go func(segment int) {
			handler := newHandler()
			handler.requests = deleteChan
			err := scanner.segmentScan(segment, handler)
			checkFatal(err)
			totalsMutex.Lock()
			totals.accumulate(handler.summary)
			totalsMutex.Unlock()
			group.Done()
		}(segment)
	}
	group.Wait()
	// Close chan to signal deleter(s) to terminate
	close(deleteChan)
	deleteGroup.Wait()

	fmt.Printf("\n")
	totals.print()
}

func (sc scanner) segmentScan(segment int, handler handler) error {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(sc.tableName),
		ProjectionExpression: aws.String(hashKey + "," + rangeKey),
		Segment:              aws.Int64(int64(segment)),
		TotalSegments:        aws.Int64(int64(sc.segments)),
		//ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	for _, arg := range flag.Args() {
		org, err := strconv.Atoi(arg)
		checkFatal(err)
		handler.orgs[org] = struct{}{}
	}

	err := sc.dynamoDB.ScanPages(input, handler.handlePage)
	if err != nil {
		return err
	}
	return nil
}

/* TODO: delete v8 schema rows for all instances */

const (
	hashKey  = "h"
	rangeKey = "r"
	valueKey = "c"
)

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
	pages    int
	orgs     map[int]struct{}
	requests chan map[string]*dynamodb.AttributeValue
	summary
}

func newHandler() handler {
	return handler{
		orgs:    map[int]struct{}{},
		summary: newSummary(),
	}
}

func (h *handler) handlePage(page *dynamodb.ScanOutput, lastPage bool) bool {
	h.pages++
	if pagesPerDot > 0 && h.pages%pagesPerDot == 0 {
		fmt.Printf(".")
	}
	for _, m := range page.Items {
		org := orgFromHash(m[hashKey].S)
		if org <= 0 {
			continue
		}
		h.counts[org]++
		if _, found := h.orgs[org]; found {
			h.requests <- m // Send attributes to the chan
		}
	}
	return true
}

func orgFromHash(hashVal *string) int {
	hashStr := aws.StringValue(hashVal)
	if hashStr == "" {
		return -1
	}
	pos := strings.Index(hashStr, "/")
	if pos < 0 { // unrecognized format
		return -1
	}
	org, err := strconv.Atoi(hashStr[:pos])
	if err != nil {
		return -1
	}
	return org
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		os.Exit(1)
	}
}

func (sc *scanner) deleteLoop(in chan map[string]*dynamodb.AttributeValue, group *sync.WaitGroup) {
	defer group.Done()
	var requests []*dynamodb.WriteRequest
	flush := func() {
		delete := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				sc.tableName: requests,
			},
		}
		level.Debug(util.Logger).Log("msg", "about to delete", "num_requests", len(requests))
		_, err := sc.dynamoDB.BatchWriteItem(delete)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to delete", "err", err)
		}
		requests = requests[:0]
	}

	for {
		keyMap, ok := <-in
		if !ok {
			if len(requests) > 0 {
				flush()
			}
			return
		}
		requests = append(requests, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: keyMap,
			},
		})
		if len(requests) >= sc.deleteBatchSize {
			flush()
		}
	}
}
