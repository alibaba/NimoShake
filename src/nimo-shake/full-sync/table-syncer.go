package full_sync

import (
	"sync"
	"fmt"

	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/protocal"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/aws"
	"nimo-shake/qps"
)

const (
	fetcherChanSize = 512
	parserChanSize  = 4096
)

type tableSyncer struct {
	id          int
	ns          utils.NS
	sourceConn  *dynamodb.DynamoDB
	targetConn  *utils.MongoConn
	fetcherChan chan *dynamodb.ScanOutput // chan between fetcher and parser
	parserChan  chan protocal.MongoData   // chan between parser and writer
	converter   protocal.Converter        // converter
}

func NewTableSyncer(id int, table string) *tableSyncer {
	sourceConn, err := utils.CreateDynamoSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Error("tableSyncer[%v] with table[%v] create dynamodb session error[%v]", id, table, err)
		return nil
	}

	targetConn, err := utils.NewMongoConn(conf.Options.TargetAddress, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("tableSyncer[%v] create mongodb session error[%v]", id, err)
		return nil
	}

	converter := protocal.NewConverter(conf.Options.ConvertType)
	if converter == nil {
		LOG.Error("tableSyncer[%v] with table[%v] create converter failed", id, table)
		return nil
	}

	return &tableSyncer{
		id:         id,
		sourceConn: sourceConn,
		targetConn: targetConn,
		converter:  converter,
		ns: utils.NS{
			Database:   conf.Options.Id,
			Collection: table,
		},
	}
}

func (ts *tableSyncer) String() string {
	return fmt.Sprintf("tableSyncer[%v] with table[%v]", ts.id, ts.ns.Collection)
}

func (ts *tableSyncer) Sync() {
	ts.fetcherChan = make(chan *dynamodb.ScanOutput, fetcherChanSize)
	ts.parserChan = make(chan protocal.MongoData, parserChanSize)

	// start fetcher to fetch all data from DynamoDB
	go ts.fetcher()

	// start parser to get data from fetcher and write into writer.
	// we can also start several parsers to accelerate
	var wgParser sync.WaitGroup
	wgParser.Add(int(conf.Options.FullDocumentParser))
	for i := 0; i < int(conf.Options.FullDocumentParser); i++ {
		go func(id int) {
			ts.parser(id)
			wgParser.Done()
		}(i)
	}

	// start writer
	var wgWriter sync.WaitGroup
	wgWriter.Add(int(conf.Options.FullDocumentConcurrency))
	for i := 0; i < int(conf.Options.FullDocumentConcurrency); i++ {
		go func(id int) {
			ds := NewDocumentSyncer(ts.id, ts.ns.Collection, id, ts.parserChan)
			ds.Run()
			wgWriter.Done()
		}(i)
	}

	LOG.Info("%s wait all parsers exiting", ts.String())
	wgParser.Wait()      // wait all parser exit
	close(ts.parserChan) // close parser channel

	LOG.Info("%s all parsers exited, wait all writers exiting", ts.String())
	wgWriter.Wait() // wait all writer exit

	LOG.Info("%s finish syncing table", ts.String())

	// start write index
	if conf.Options.FullEnableIndexPrimary || conf.Options.FullEnableIndexUser {
		// enable index
		LOG.Info("%s try to write index", ts.String())
		ix := NewIndex(ts.sourceConn, ts.targetConn, ts.ns)
		if err := ix.CreateIndex(); err != nil {
			LOG.Error("%s create index failed[%v]", ts.String(), err)
		}
		LOG.Info("%s finish syncing index", ts.String())
	}
}

func (ts *tableSyncer) Close() {
	// TODO, dynamo-session doesn't have close function?
	ts.targetConn.Close()
}

func (ts *tableSyncer) fetcher() {
	LOG.Info("%s start fetcher", ts.String())

	qos := qps.StartQoS(int(conf.Options.QpsFull))
	defer qos.Close()

	// init nil
	var previousKey map[string]*dynamodb.AttributeValue
	for {
		<-qos.Bucket

		out, err := ts.sourceConn.Scan(&dynamodb.ScanInput{
			TableName:         aws.String(ts.ns.Collection),
			ExclusiveStartKey: previousKey,
			Limit:             aws.Int64(conf.Options.QpsFullBatchNum),
		})
		if err != nil {
			// TODO check network error and retry
			LOG.Crashf("%s fetcher scan failed[%v]", ts.String(), err)
		}

		// LOG.Info(*out.Count)

		// pass result to parser
		ts.fetcherChan <- out

		previousKey = out.LastEvaluatedKey
		if previousKey == nil {
			// complete
			break
		}
	}

	LOG.Info("%s close fetcher", ts.String())
	close(ts.fetcherChan)
}

func (ts *tableSyncer) parser(id int) {
	LOG.Info("%s start parser[%v]", ts.String(), id)

	for {
		data, ok := <-ts.fetcherChan
		if !ok {
			break
		}

		LOG.Debug("%s parser[%v] read data[%v]", ts.String(), id, data)

		list := data.Items
		for _, ele := range list {
			out, err := ts.converter.Run(ele)
			if err != nil {
				LOG.Crashf("%s parser[%v] parse ele[%v] failed[%v]", ts.String(), id, ele, err)
			}

			ts.parserChan <- out
		}
	}
	LOG.Info("%s close parser", ts.String())
}
