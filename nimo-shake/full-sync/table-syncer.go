package full_sync

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"

	utils "nimo-shake/common"
	conf "nimo-shake/configure"
	"nimo-shake/protocal"
	"nimo-shake/qps"
	"nimo-shake/writer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
)

const (
	fetcherChanSize = 1024
	parserChanSize  = 81920
)

type tableSyncer struct {
	id                  int
	ns                  utils.NS
	sourceConn          *dynamodb.DynamoDB
	sourceTableDescribe *dynamodb.TableDescription
	fetcherChan         chan *dynamodb.ScanOutput // chan between fetcher and parser
	parserChan          chan interface{}          // chan between parser and writer
	converter           protocal.Converter        // converter
	collectionMetric    *utils.CollectionMetric
}

func NewTableSyncer(id int, table string, collectionMetric *utils.CollectionMetric) *tableSyncer {
	sourceConn, err := utils.CreateDynamoSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Error("tableSyncer[%v] with table[%v] create dynamodb session error[%v]", id, table, err)
		return nil
	}

	// describe source table
	tableDescription, err := sourceConn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		LOG.Error("tableSyncer[%v] with table[%v] describe failed[%v]", id, table, err)
		return nil
	}

	converter := protocal.NewConverter(conf.Options.ConvertType)
	if converter == nil {
		LOG.Error("tableSyncer[%v] with table[%v] create converter failed", id, table)
		return nil
	}

	return &tableSyncer{
		id:                  id,
		sourceConn:          sourceConn,
		sourceTableDescribe: tableDescription.Table,
		converter:           converter,
		ns: utils.NS{
			Database:   conf.Options.Id,
			Collection: table,
		},
		collectionMetric: collectionMetric,
	}
}

func (ts *tableSyncer) String() string {
	return fmt.Sprintf("tableSyncer[%v] with table[%v]", ts.id, ts.ns.Collection)
}

func (ts *tableSyncer) Sync() {
	ts.fetcherChan = make(chan *dynamodb.ScanOutput, fetcherChanSize)
	ts.parserChan = make(chan interface{}, parserChanSize)

	targetWriter := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, ts.ns, conf.Options.LogLevel)
	if targetWriter == nil {
		LOG.Crashf("%s create writer failed", ts)
		return
	}
	// create table and index with description
	if err := targetWriter.CreateTable(ts.sourceTableDescribe); err != nil {
		LOG.Crashf("%s create table failed: %v", ts, err)
		return
	}

	// wait dynamo proxy to sync cache
	time.Sleep(10 * time.Second)

	if conf.Options.SyncSchemaOnly {
		LOG.Info("sync_schema_only enabled, %s exits", ts)
		return
	}

	// total table item count
	totalCount := ts.count()

	ts.collectionMetric.CollectionStatus = utils.StatusProcessing
	ts.collectionMetric.TotalCount = totalCount

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
			LOG.Info("%s create document syncer with id[%v]", ts, id)
			ds := NewDocumentSyncer(ts.id, ts.ns.Collection, id, ts.parserChan, ts.sourceTableDescribe,
				ts.collectionMetric)
			ds.Run()
			LOG.Info("%s document syncer with id[%v] exit", ts, id)
			wgWriter.Done()
		}(i)
	}

	LOG.Info("%s wait all parsers exiting", ts.String())
	wgParser.Wait()      // wait all parser exit
	close(ts.parserChan) // close parser channel

	LOG.Info("%s all parsers exited, wait all writers exiting", ts.String())
	wgWriter.Wait() // wait all writer exit

	ts.collectionMetric.CollectionStatus = utils.StatusFinish
	LOG.Info("%s finish syncing table", ts.String())
}

func (ts *tableSyncer) Close() {
	// TODO, dynamo-session doesn't have close function?
}

func (ts *tableSyncer) fetcher() {
	LOG.Info("%s start fetcher with %v reader", ts.String(), conf.Options.FullReadConcurrency)

	qos := qps.StartQoS(int(conf.Options.QpsFull))
	defer qos.Close()

	var wg sync.WaitGroup
	wg.Add(int(conf.Options.FullReadConcurrency))
	for i := 0; i < int(conf.Options.FullReadConcurrency); i++ {
		go func(segmentId int64) {
			LOG.Info("%s start reader[%v]", ts.String(), segmentId)
			defer LOG.Info("%s stop reader[%v]", ts.String(), segmentId)

			// init nil
			var previousKey map[string]*dynamodb.AttributeValue
			for {
				<-qos.Bucket

				startT := time.Now()
				scanInput := &dynamodb.ScanInput{
					TableName:         aws.String(ts.ns.Collection),
					TotalSegments:     aws.Int64(int64(conf.Options.FullReadConcurrency)),
					Segment:           aws.Int64(segmentId),
					ExclusiveStartKey: previousKey,
					Limit:             aws.Int64(conf.Options.QpsFullBatchNum),
				}
				if len(conf.Options.FullFilterExpression) > 0 {
					scanInput.FilterExpression = aws.String(conf.Options.FullFilterExpression)
					scanInput.ExpressionAttributeValues = utils.ParseAttributes(conf.Options.FullFilterAttributeValues)
				}
				out, err := ts.sourceConn.Scan(scanInput)
				if err != nil {
					// TODO check network error and retry
					if aerr, ok := err.(awserr.Error); ok {

						switch aerr.Code() {
						case dynamodb.ErrCodeProvisionedThroughputExceededException:
							LOG.Warn("%s fetcher reader[%v] recv ProvisionedThroughputExceededException continue",
								ts.String(), segmentId)
							time.Sleep(5 * time.Second)
							continue

						case request.ErrCodeSerialization:
							LOG.Warn("%s fetcher reader[%v] recv SerializationError[%v] continue",
								ts.String(), segmentId, err)
							time.Sleep(5 * time.Second)
							continue

						case request.ErrCodeRequestError, request.CanceledErrorCode,
							request.ErrCodeResponseTimeout, request.HandlerResponseTimeout,
							request.WaiterResourceNotReadyErrorCode, request.ErrCodeRead:
							LOG.Warn("%s fetcher reader[%v] recv Error[%v] continue",
								ts.String(), segmentId, err)
							time.Sleep(5 * time.Second)
							continue

						default:
							LOG.Crashf("%s fetcher scan failed[%v] errcode[%v]", ts.String(), err, aerr.Code())
						}
					} else {
						LOG.Crashf("%s fetcher scan failed[%v]", ts.String(), err)
					}
				}
				scanDuration := time.Since(startT)

				// LOG.Info(*out.Count)

				// pass result to parser
				startT = time.Now()
				ts.fetcherChan <- out
				writeFetcherChan := time.Since(startT)

				LOG.Info("%s fetcher reader[%v] ts.fetcherChan.len[%v] "+
					"scanTime[%v] scanCount[%v] writeFetcherChanTime[%v]",
					ts.String(), segmentId, len(ts.fetcherChan), scanDuration, *out.Count, writeFetcherChan)

				previousKey = out.LastEvaluatedKey
				if previousKey == nil {
					// complete
					break
				}
			}
			wg.Done()
		}(int64(i))
	}
	wg.Wait()

	LOG.Info("%s close fetcher", ts.String())
	close(ts.fetcherChan)
}

func (ts *tableSyncer) parser(id int) {
	LOG.Info("%s start parser[%v]", ts.String(), id)

	for {
		startT := time.Now()
		data, ok := <-ts.fetcherChan
		if !ok {
			break
		}
		readFetcherChanDuration := time.Since(startT)

		LOG.Debug("%s parser[%v] read data[%v]", ts.String(), id, data)

		var parserDuration, writeParseChanDuration time.Duration = 0, 0

		list := data.Items
		for _, ele := range list {
			startT = time.Now()
			out, err := ts.converter.Run(ele)
			parserDuration = parserDuration + time.Since(startT)
			if err != nil {
				LOG.Crashf("%s parser[%v] parse ele[%v] failed[%v]", ts.String(), id, ele, err)
			}

			startT = time.Now()
			ts.parserChan <- out
			writeParseChanDuration = writeParseChanDuration + time.Since(startT)
		}

		LOG.Info("%s parser parser[%v] readFetcherChanTime[%v] parserTime[%v]"+
			" writeParseChantime[%v] parserChan.len[%v]",
			ts.String(), id, readFetcherChanDuration, parserDuration, writeParseChanDuration, len(ts.parserChan))

	}
	LOG.Info("%s close parser", ts.String())
}

func (ts *tableSyncer) count() uint64 {
	return uint64(*ts.sourceTableDescribe.ItemCount)
}
