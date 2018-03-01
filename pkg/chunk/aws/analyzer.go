package chunk

import (
	"fmt"

	"github.com/prometheus/common/model"
)

func Analyze(from, through model.Time, userID string) {
	fmt.Printf("Start\n")

	input := &dynamodb.QueryInput{
		TableName: aws.String(query.TableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(query.HashValue)},
				},
				ComparisonOperator: aws.String(dynamodb.ComparisonOperatorEq),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
}
