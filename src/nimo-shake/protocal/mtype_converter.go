package protocal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	LOG "github.com/vinllen/log4go"
	"time"
)

type MTypeConverter struct {
}


func (tc *MTypeConverter) Run(input map[string]*dynamodb.AttributeValue) (interface{}, error) {

	funcStartT := time.Now()

	out := new(interface{})
	if err := dynamodbattribute.UnmarshalMap(input, out); err == nil {
		LOG.Debug("Run_func input[%v] out[%v] duration[%v]", input, *out, time.Since(funcStartT))

		return RawData{10, *out}, nil
	} else {
		LOG.Debug("Run_func input[%v] out[%v] err[%v]", input, *out, err)
	}

	return RawData{}, fmt.Errorf("parse failed, return nil")
}