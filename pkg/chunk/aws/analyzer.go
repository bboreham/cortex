package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (a storageClient) AnalyzeBucket(tableName, bucketHash string) error {
	ctx := context.TODO()

	input := &dynamodb.QueryInput{
		TableName: aws.String(tableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(bucketHash)},
				},
				ComparisonOperator: aws.String(dynamodb.ComparisonOperatorEq),
			},
		},
	}

	count := 0

	request := a.queryRequest(ctx, input)

	for page := request; page != nil; page = page.NextPage() {
		resp, err := a.queryPage(ctx, input, page)
		if err != nil {
			return err
		}
		//fmt.Printf("%d entries returned\n", resp.Len())
		for i := 0; i < resp.Len(); i++ {
			// RangeValue is a sha; Value is the labelset
			fmt.Printf("%s\n", resp.Value(i))
			count++
		}
	}

	return nil
}
