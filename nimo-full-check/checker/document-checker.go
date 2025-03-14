package checker

import (
	"context"
	"encoding/json"
	"fmt"
	conf "nimo-full-check/configure"
	"os"
	"reflect"

	shakeUtils "nimo-shake/common"
	"nimo-shake/protocal"
	shakeQps "nimo-shake/qps"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func convertToMap(data interface{}) interface{} {
	switch v := data.(type) {
	case primitive.D:
		result := make(map[string]interface{})
		for _, elem := range v {
			result[elem.Key] = convertToMap(elem.Value)
		}
		return result

	case primitive.ObjectID:
		return v.Hex()

	case []interface{}:
		var newSlice []interface{}
		for _, item := range v {
			newSlice = append(newSlice, convertToMap(item))
		}
		return newSlice

	default:
		return v
	}
}

func interIsEqual(dynamoData, convertedMongo interface{}) bool {

	//convertedMongo := convertToMap(mongoData)
	if m, ok := convertedMongo.(map[string]interface{}); ok {
		delete(m, "_id")
	} else {
		LOG.Warn("don't have _id in mongodb document")
		return false
	}

	opts := cmp.Options{
		cmp.FilterPath(func(p cmp.Path) bool {
			if len(p) > 0 {
				switch p.Last().(type) {
				case cmp.MapIndex, cmp.SliceIndex:
					return true
				}
			}
			return false
		}, cmp.Transformer("NormalizeNumbers", func(in interface{}) interface{} {
			switch v := in.(type) {
			case int, int32, int64, float32, float64:
				return reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()
			default:
				return in
			}
		})),
	}
	// LOG.Warn("tmp2 %v", cmp.Diff(dynamoData, convertedMongo, opts))

	return cmp.Equal(dynamoData, convertedMongo, opts)
}

const (
	fetcherChanSize = 512
	parserChanSize  = 4096
)

type KeyUnion struct {
	name  string
	tp    string
	union string
}

type DocumentChecker struct {
	id                 int
	ns                 shakeUtils.NS
	sourceConn         *dynamodb.DynamoDB
	mongoClient        *shakeUtils.MongoCommunityConn
	fetcherChan        chan *dynamodb.ScanOutput // chan between fetcher and parser
	parserChan         chan protocal.RawData     // chan between parser and writer
	converter          protocal.Converter        // converter
	sampler            *Sample                   // use to sample
	primaryKeyWithType KeyUnion
	sortKeyWithType    KeyUnion
}

func NewDocumentChecker(id int, table string, dynamoSession *dynamodb.DynamoDB) *DocumentChecker {
	// check mongodb connection
	mongoClient, err := shakeUtils.NewMongoCommunityConn(conf.Opts.TargetAddress, shakeUtils.ConnectModePrimary, true)
	if err != nil {
		LOG.Crashf("documentChecker[%v] with table[%v] connect mongodb[%v] failed[%v]", id, table,
			conf.Opts.TargetAddress, err)
	}

	converter := protocal.NewConverter(conf.Opts.ConvertType)
	if converter == nil {
		LOG.Error("documentChecker[%v] with table[%v] create converter failed", id, table)
		return nil
	}

	return &DocumentChecker{
		id:          id,
		sourceConn:  dynamoSession,
		mongoClient: mongoClient,
		converter:   converter,
		ns: shakeUtils.NS{
			Collection: table,
			Database:   conf.Opts.Id,
		},
	}
}

func (dc *DocumentChecker) String() string {
	return fmt.Sprintf("documentChecker[%v] with table[%s]", dc.id, dc.ns)
}

func (dc *DocumentChecker) Run() {
	// check outline
	if err := dc.checkOutline(); err != nil {
		LOG.Crashf("%s check outline failed[%v]", dc.String(), err)
	}

	LOG.Info("%s check outline finish, starts checking details", dc.String())

	dc.fetcherChan = make(chan *dynamodb.ScanOutput, fetcherChanSize)
	dc.parserChan = make(chan protocal.RawData, parserChanSize)

	// start fetcher to fetch all data from DynamoDB
	go dc.fetcher()

	// start parser to get data from fetcher and write into exector.
	go dc.parser()

	// start executor to check
	dc.executor()
}

func (dc *DocumentChecker) fetcher() {
	LOG.Info("%s start fetcher", dc.String())

	qos := shakeQps.StartQoS(int(conf.Opts.QpsFull))
	defer qos.Close()

	// init nil
	var previousKey map[string]*dynamodb.AttributeValue
	for {
		<-qos.Bucket

		out, err := dc.sourceConn.Scan(&dynamodb.ScanInput{
			TableName:         aws.String(dc.ns.Collection),
			ExclusiveStartKey: previousKey,
			Limit:             aws.Int64(conf.Opts.QpsFullBatchNum),
		})
		if err != nil {
			// TODO check network error and retry
			LOG.Crashf("%s fetcher scan failed[%v]", dc.String(), err)
		}

		// pass result to parser
		dc.fetcherChan <- out

		previousKey = out.LastEvaluatedKey
		if previousKey == nil {
			// complete
			break
		}
	}

	LOG.Info("%s close fetcher", dc.String())
	close(dc.fetcherChan)
}

func (dc *DocumentChecker) parser() {
	LOG.Info("%s start parser", dc.String())

	for {
		data, ok := <-dc.fetcherChan
		if !ok {
			break
		}

		LOG.Debug("%s reads data[%v]", dc.String(), data)

		list := data.Items
		for _, ele := range list {
			out, err := dc.converter.Run(ele)
			if err != nil {
				LOG.Crashf("%s parses ele[%v] failed[%v]", dc.String(), ele, err)
			}

			// sample
			if dc.sampler.Hit() == false {
				continue
			}

			dc.parserChan <- out.(protocal.RawData)
		}
	}

	LOG.Info("%s close parser", dc.String())
	close(dc.parserChan)
}

func (dc *DocumentChecker) executor() {
	LOG.Info("%s start executor", dc.String())

	diffFile := fmt.Sprintf("%s/%s", conf.Opts.DiffOutputFile, dc.ns.Collection)
	f, err := os.Create(diffFile)
	if err != nil {
		LOG.Crashf("%s create diff output file[%v] failed", dc.String(), diffFile)
		return
	}

	for {
		data, ok := <-dc.parserChan
		if !ok {
			break
		}

		//var query map[string]interface{}
		query := make(map[string]interface{})
		if dc.primaryKeyWithType.name != "" {
			// find by union key
			if conf.Opts.ConvertType == shakeUtils.ConvertMTypeChange {
				query[dc.primaryKeyWithType.name] = data.Data.(map[string]interface{})[dc.primaryKeyWithType.name]
			} else {
				LOG.Crashf("unknown convert type[%v]", conf.Opts.ConvertType)
			}
		}
		if dc.sortKeyWithType.name != "" {
			if conf.Opts.ConvertType == shakeUtils.ConvertMTypeChange {
				query[dc.sortKeyWithType.name] = data.Data.(map[string]interface{})[dc.sortKeyWithType.name]
			} else {
				LOG.Crashf("unknown convert type[%v]", conf.Opts.ConvertType)
			}
		}

		LOG.Info("query: %v", query)

		// query
		var output, outputMap interface{}
		isSame := true
		err := dc.mongoClient.Client.Database(dc.ns.Database).Collection(dc.ns.Collection).
			FindOne(context.TODO(), query).Decode(&output)
		if err != nil {
			err = fmt.Errorf("target query failed[%v][%v][%v]", err, output, query)
			LOG.Error("%s %v", dc.String(), err)
		} else {
			outputMap = convertToMap(output)
			isSame = interIsEqual(data.Data, outputMap)
		}

		inputJson, _ := json.Marshal(data.Data)
		outputJson, _ := json.Marshal(outputMap)
		if err != nil {
			f.WriteString(fmt.Sprintf("compare src[%s] to dst[%s] failed: %v\n", inputJson, outputJson, err))
		} else if isSame == false {
			LOG.Warn("compare src[%s] and dst[%s] failed", inputJson, outputJson)
			f.WriteString(fmt.Sprintf("src[%s] != dst[%s]\n", inputJson, outputJson))
		}
	}

	LOG.Info("%s close executor", dc.String())
	f.Close()

	// remove file if size == 0
	if fi, err := os.Stat(diffFile); err != nil {
		LOG.Warn("stat diffFile[%v] failed[%v]", diffFile, err)
		return
	} else if fi.Size() == 0 {
		if err := os.Remove(diffFile); err != nil {
			LOG.Warn("remove diffFile[%v] failed[%v]", diffFile, err)
		}
	}
}

func (dc *DocumentChecker) checkOutline() error {
	// describe dynamodb table
	out, err := dc.sourceConn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(dc.ns.Collection),
	})
	if err != nil {
		return fmt.Errorf("describe table failed[%v]", err)
	}

	LOG.Info("describe table[%v] result: %v", dc.ns.Collection, out)

	// 1. check total number
	// dynamo count
	dynamoCount := out.Table.ItemCount

	// mongo count
	cnt, err := dc.mongoClient.Client.Database(dc.ns.Database).Collection(dc.ns.Collection).CountDocuments(context.Background(), bson.M{})
	if err != nil {
		return fmt.Errorf("get mongo count failed[%v]", err)
	}

	if *dynamoCount != cnt {
		// return fmt.Errorf("dynamo count[%v] != mongo count[%v]", *dynamoCount, cnt)
		LOG.Warn("dynamo count[%v] != mongo count[%v]", *dynamoCount, cnt)
	}

	// set sampler
	dc.sampler = NewSample(conf.Opts.Sample, cnt)

	// 2. check index
	// TODO

	// parse index
	// parse primary key with sort key
	allIndexes := out.Table.AttributeDefinitions
	primaryIndexes := out.Table.KeySchema

	// parse index type
	parseMap := shakeUtils.ParseIndexType(allIndexes)
	primaryKey, sortKey, err := shakeUtils.ParsePrimaryAndSortKey(primaryIndexes, parseMap)
	if err != nil {
		return fmt.Errorf("parse primary and sort key failed[%v]", err)
	}
	dc.primaryKeyWithType = KeyUnion{
		name:  primaryKey,
		tp:    parseMap[primaryKey],
		union: fmt.Sprintf("%s.%s", primaryKey, parseMap[primaryKey]),
	}
	dc.sortKeyWithType = KeyUnion{
		name:  sortKey,
		tp:    parseMap[sortKey],
		union: fmt.Sprintf("%s.%s", sortKey, parseMap[sortKey]),
	}

	return nil
}
