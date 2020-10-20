package writer

import (
	"fmt"
	"bytes"
	"net/http"
	"time"

	"nimo-shake/common"
	"nimo-shake/configure"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoProxyWriter struct {
	Name string
	svc  *dynamodb.DynamoDB
	ns   utils.NS
}

func NewDynamoProxyWriter(name, address string, ns utils.NS, logLevel string) *DynamoProxyWriter {
	config := &aws.Config{
		Region:     aws.String("us-east-2"), // meaningless
		Endpoint:   aws.String(address),
		MaxRetries: aws.Int(3),
		DisableSSL: aws.Bool(true),
		HTTPClient: &http.Client{
			Timeout: time.Duration(5000) * time.Millisecond,
		},
	}

	var err error
	sess, err := session.NewSession(config)
	if err != nil {
		LOG.Crashf("create dynamo connection error[%v]", err)
		return nil
	}

	var svc *dynamodb.DynamoDB
	if logLevel == "debug" {
		svc = dynamodb.New(sess, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))
	} else {
		svc = dynamodb.New(sess)
	}

	return &DynamoProxyWriter{
		Name: name,
		svc:  svc,
		ns:   ns,
	}
}

func (dpw *DynamoProxyWriter) String() string {
	return dpw.Name
}

func (dpw *DynamoProxyWriter) GetSession() interface{} {
	return dpw.svc
}

func (dpw *DynamoProxyWriter) CreateTable(tableDescribe *dynamodb.TableDescription) error {
	createTableInput := &dynamodb.CreateTableInput{
		AttributeDefinitions: tableDescribe.AttributeDefinitions,
		KeySchema:            tableDescribe.KeySchema,
		TableName:            tableDescribe.TableName,
	}

	if conf.Options.FullEnableIndexUser {
		// convert []*GlobalSecondaryIndexDescription => []*GlobalSecondaryIndex
		gsiList := make([]*dynamodb.GlobalSecondaryIndex, 0, len(tableDescribe.GlobalSecondaryIndexes))
		for _, gsiDesc := range tableDescribe.GlobalSecondaryIndexes {
			gsiList = append(gsiList, &dynamodb.GlobalSecondaryIndex{
				IndexName: gsiDesc.IndexName,
				KeySchema: gsiDesc.KeySchema,
				Projection: gsiDesc.Projection,
				// ProvisionedThroughput: gsiDesc.ProvisionedThroughput,
			})
		}
		createTableInput.SetGlobalSecondaryIndexes(gsiList)

		// convert []*LocalSecondaryIndexDescription => []*LocalSecondaryIndex
		lsiList := make([]*dynamodb.LocalSecondaryIndex, 0, len(tableDescribe.LocalSecondaryIndexes))
		for _, lsiDesc := range tableDescribe.LocalSecondaryIndexes {
			lsiList = append(lsiList, &dynamodb.LocalSecondaryIndex{
				IndexName: lsiDesc.IndexName,
				KeySchema: lsiDesc.KeySchema,
				Projection: lsiDesc.Projection,
			})
		}
		createTableInput.SetLocalSecondaryIndexes(lsiList)
	}

	_, err := dpw.svc.CreateTable(createTableInput)
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
		return fmt.Errorf("create table[%v] fail: check ready fail", dpw.ns.Collection)
	}

	return nil
}

func (dpw *DynamoProxyWriter) DropTable() error {
	_, err := dpw.svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(dpw.ns.Collection),
	})
	return err
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
		RequestItems: map[string][]*dynamodb.WriteRequest{
			dpw.ns.Collection: request,
		},
	})
	return err
}

func (dpw *DynamoProxyWriter) Close() {

}

// input type is map[string]*dynamodb.AttributeValue
func (dpw *DynamoProxyWriter) Insert(input []interface{}, index []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	request := make([]*dynamodb.WriteRequest, len(index))
	for i, ele := range input {
		request[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: ele.(map[string]*dynamodb.AttributeValue),
			},
		}
	}

	_, err := dpw.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			dpw.ns.Collection: request,
		},
	})

	if err != nil && utils.DynamoIgnoreError(err, "i", true) {
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
		RequestItems: map[string][]*dynamodb.WriteRequest{
			dpw.ns.Collection: request,
		},
	})

	if utils.DynamoIgnoreError(err, "d", true) {
		LOG.Warn("%s ignore error[%v] when delete", dpw, err)
		return nil
	}

	return err
}

// upsert
func (dpw *DynamoProxyWriter) Update(input []interface{}, index []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	// fmt.Println(input, index)

	for i := range input {
		val := input[i].(map[string]*dynamodb.AttributeValue)
		key := index[i].(map[string]*dynamodb.AttributeValue)

		// why no update interface like BatchWriteItem !!!!
		// generate new map(expression-attribute-values) and expression(update-expression)
		newMap := make(map[string]*dynamodb.AttributeValue, len(val))
		expressionBuffer := new(bytes.Buffer)
		expressionBuffer.WriteString("SET")
		cnt := 1
		for k, v := range val {
			newKey := fmt.Sprintf(":v%d", cnt)
			newMap[newKey] = v

			if cnt == 1 {
				expressionBuffer.WriteString(fmt.Sprintf(" %s=%s", k, newKey))
			} else {
				expressionBuffer.WriteString(fmt.Sprintf(",%s=%s", k, newKey))
			}

			cnt++
		}

		// fmt.Println(newMap)
		_, err := dpw.svc.UpdateItem(&dynamodb.UpdateItemInput{
			TableName:                 aws.String(dpw.ns.Collection),
			Key:                       key,
			UpdateExpression:          aws.String(expressionBuffer.String()),
			ExpressionAttributeValues: newMap,
		})
		if err != nil && utils.DynamoIgnoreError(err, "u", true) {
			LOG.Warn("%s ignore error[%v] when insert", dpw, err)
			return nil
		}
	}

	return nil
}
