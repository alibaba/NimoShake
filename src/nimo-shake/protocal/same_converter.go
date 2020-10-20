package protocal

import "github.com/aws/aws-sdk-go/service/dynamodb"

type SameConverter struct {
}

func (sc *SameConverter) Run(input map[string]*dynamodb.AttributeValue) (interface{}, error) {
	return input, nil
}