package checker

import (
	"fmt"
	"os"
	"encoding/json"

	shakeUtils "nimo-shake/common"
	shakeQps "nimo-shake/qps"
	"nimo-full-check/configure"
	"nimo-shake/protocal"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/vinllen/mgo/bson"
)

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
	mongoClient        *shakeUtils.MongoConn
	fetcherChan        chan *dynamodb.ScanOutput // chan between fetcher and parser
	parserChan         chan protocal.MongoData   // chan between parser and writer
	converter          protocal.Converter        // converter
	sampler            *Sample                   // use to sample
	primaryKeyWithType KeyUnion
	sortKeyWithType    KeyUnion
}

func NewDocumentChecker(id int, table string, dynamoSession *dynamodb.DynamoDB) *DocumentChecker {
	// check mongodb connection
	mongoClient, err := shakeUtils.NewMongoConn(conf.Opts.TargetAddress, shakeUtils.ConnectModePrimary, true)
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
	dc.parserChan = make(chan protocal.MongoData, parserChanSize)

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

			dc.parserChan <- out
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

		input := data.Data.(bson.M)
		var query bson.M
		if dc.primaryKeyWithType.name != "" && dc.sortKeyWithType.name != "" {
			// find by union key
			query = make(bson.M, 0)
			query[dc.primaryKeyWithType.union] = data.Data.(bson.M)[dc.primaryKeyWithType.name].(bson.M)[dc.primaryKeyWithType.tp]
			query[dc.sortKeyWithType.union] = data.Data.(bson.M)[dc.sortKeyWithType.name].(bson.M)[dc.sortKeyWithType.tp]
		} else {
			// find by whole doc
			query = data.Data.(bson.M)
		}

		// query
		var output bson.M
		isSame := true
		err := dc.mongoClient.Session.DB(dc.ns.Database).C(dc.ns.Collection).Find(query).One(&output)
		if err != nil {
			err = fmt.Errorf("target query failed[%v]", err)
			LOG.Error("%s %v", dc.String(), err)
		} else {
			isSame, err = shakeUtils.CompareBson(input, output)
			if err != nil {
				err = fmt.Errorf("bson compare failed[%v]", err)
				LOG.Error("%s %v", dc.String(), err)
			}
		}

		inputJson, _ := json.Marshal(input)
		outputJson, _ := json.Marshal(output)
		if err != nil {
			f.WriteString(fmt.Sprintf("compare src[%s] to dst[%s] failed: %v\n", inputJson, outputJson, err))
		} else if isSame == false {
			LOG.Warn("compare src[%v] and dst[%v] failed", inputJson, outputJson)
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
	cnt, err := dc.mongoClient.Session.DB(dc.ns.Database).C(dc.ns.Collection).Count()
	if err != nil {
		return fmt.Errorf("get mongo count failed[%v]", err)
	}

	if *dynamoCount != int64(cnt) {
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
