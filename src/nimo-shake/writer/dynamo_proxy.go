package writer

import (
	"nimo-shake/common"
	"github.com/aws/aws-sdk-go/aws"
	"net/http"
	"time"
	"github.com/aws/aws-sdk-go/aws/session"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"fmt"
)

type DynamoProxyWriter struct {
	Name string
	svc  *dynamodb.DynamoDB
	ns   utils.NS
}

func NewDynamoProxyWriter(name, address string, ns utils.NS, logLevel string) *DynamoProxyWriter {
	config := &aws.Config{
		Endpoint:   aws.String(address),
		MaxRetries: aws.Int(3),
		HTTPClient: &http.Client{
			Timeout: time.Duration(5000) * time.Millisecond,
		},
	}

	var err error
	session, err := session.NewSession(config)
	if err != nil {
		LOG.Crashf("create dynamo connection error[%v]", err)
		return nil
	}

	var svc *dynamodb.DynamoDB
	if logLevel == "debug" {
		svc = dynamodb.New(session, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))
	} else {
		svc = dynamodb.New(session)
	}

	return &DynamoProxyWriter{
		Name: name,
		svc:  svc,
		ns:   ns,
	}
}

func (dpw *DynamoProxyWriter) CreateTable(ns utils.NS, tableDescribe *dynamodb.TableDescription) error {
	_, err := dpw.svc.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: tableDescribe.AttributeDefinitions,
		KeySchema: tableDescribe.KeySchema,
		TableName: tableDescribe.TableName,
	})
	if err != nil {
		LOG.Error("create table[%v] fail: %v", tableDescribe.TableName, err)
		return err
	}

	checkReady := func() bool {
		// check table is ready
		out, err := dpw.svc.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: tableDescribe.TableName,
		})
		if err != nil {
			LOG.Warn("create table[%v] ok but describe failed: %v", tableDescribe.TableName, err)
			return true
		}

		if *out.Table.TableStatus != "ACTIVE" {
			LOG.Warn("create table[%v] ok but describe not ready: %v", tableDescribe.TableName, *out.Table.TableStatus)
			return true
		}
		return false
	}

	// check with retry 5 times and 1s gap
	ok := utils.CallbackRetry(5, 1000, checkReady)
	if !ok {
		return fmt.Errorf("create table[%v] fail: check ready fail")
	}

	return nil
}

func (dpw *DynamoProxyWriter) String() string {
	return dpw.Name
}

func (dpw *DynamoProxyWriter) WriteBulk(input []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	// convert to WriteRequest
	request := make([]*dynamodb.WriteRequest, len(input))
	for i, ele := range input {
		request[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: ele.(map[string]*dynamodb.AttributeValue),
			},
		}
	}

	_, err := dpw.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest {
			dpw.ns.Collection: request,
		},
	})
	return err
}

// do nothing
func (dpw *DynamoProxyWriter) CreateIndex(tableDescribe *dynamodb.TableDescription) error {
	return nil
}

func (dpw *DynamoProxyWriter) Close() {

}

// input type is map[string]*dynamodb.AttributeValue
func (dpw *DynamoProxyWriter) Insert(input []interface{}, index []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	request := make([]*dynamodb.WriteRequest, len(index))
	for i, ele := range index {
		request[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: ele.(map[string]*dynamodb.AttributeValue),
			},
		}
	}

	_, err := dpw.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest {
			dpw.ns.Collection: request,
		},
	})

	if utils.DynamoIgnoreError(err, "i", true) {
		LOG.Warn("%s ignore error[%v] when insert", dpw, err)
		return nil
	}

	return err
}

func (dpw *DynamoProxyWriter) Delete(index []interface{}) error {
	if len(index) == 0 {
		return nil
	}

	request := make([]*dynamodb.WriteRequest, len(index))
	for i, ele := range index {
		request[i] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: ele.(map[string]*dynamodb.AttributeValue),
			},
		}
	}

	_, err := dpw.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest {
			dpw.ns.Collection: request,
		},
	})

	if utils.DynamoIgnoreError(err, "d", true) {
		LOG.Warn("%s ignore error[%v] when delete", dpw, err)
		return nil
	}

	return err
}

func (dpw *DynamoProxyWriter) Update(input []interface{}, index []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	request := make([]*dynamodb.WriteRequest, len(index))
	for i, ele := range index {
		request[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: ele.(map[string]*dynamodb.AttributeValue),
			},
		}
	}

	_, err := dpw.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest {
			dpw.ns.Collection: request,
		},
	})

	if utils.DynamoIgnoreError(err, "u", true) {
		LOG.Warn("%s ignore error[%v] when insert", dpw, err)
		return nil
	}

	return err
}