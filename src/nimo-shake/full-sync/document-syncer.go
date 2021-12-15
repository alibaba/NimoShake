package full_sync

import (
	"fmt"
	"time"

	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/writer"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	"nimo-shake/protocal"
	"sync/atomic"
)

const (
	batchSize    = 2 * utils.MB // mongodb limit: 16MB
	batchTimeout = 1            // seconds
)

var (
	UT_TestDocumentSyncer      = false
	UT_TestDocumentSyncer_Chan chan []interface{}
)

/*------------------------------------------------------*/
// one document link corresponding to one documentSyncer
type documentSyncer struct {
	tableSyncerId    int
	id               int // documentSyncer id
	ns               utils.NS
	inputChan        chan interface{} // parserChan in table-syncer
	writer           writer.Writer
	collectionMetric *utils.CollectionMetric
}

func NewDocumentSyncer(tableSyncerId int, table string, id int, inputChan chan interface{},
	tableDescribe *dynamodb.TableDescription, collectionMetric *utils.CollectionMetric) *documentSyncer {
	ns := utils.NS{
		Database:   conf.Options.Id,
		Collection: table,
	}

	w := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, ns, conf.Options.LogLevel)
	if w == nil {
		LOG.Crashf("tableSyncer[%v] documentSyncer[%v] create writer failed", tableSyncerId, table)
	}

	w.PassTableDesc(tableDescribe)

	return &documentSyncer{
		tableSyncerId:    tableSyncerId,
		id:               id,
		inputChan:        inputChan,
		writer:           w,
		ns:               ns,
		collectionMetric: collectionMetric,
	}
}

func (ds *documentSyncer) String() string {
	return fmt.Sprintf("tableSyncer[%v] documentSyncer[%v] ns[%v]", ds.tableSyncerId, ds.id, ds.ns)
}

func (ds *documentSyncer) Close() {
	ds.writer.Close()
}

func (ds *documentSyncer) Run() {
	batchNumber := int(conf.Options.FullDocumentWriteBatch)
	LOG.Info("%s start with batchSize[%v]", ds.String(), batchNumber)

	var data interface{}
	var ok bool
	batchGroup := make([]interface{}, 0, batchNumber)
	timeout := false
	batchGroupSize := 0
	exit := false
	for {
		StartT := time.Now()
		select {
		case data, ok = <-ds.inputChan:
			if !ok {
				exit = true
				LOG.Info("%s channel already closed, flushing cache and exiting...", ds.String())
			}
		case <-time.After(time.Second * batchTimeout):
			// timeout
			timeout = true
			data = nil
		}
		readParserChanDuration := time.Since(StartT)

		LOG.Debug("exit[%v], timeout[%v], len(batchGroup)[%v], batchGroupSize[%v], data[%v]", exit, timeout,
			len(batchGroup), batchGroupSize, data)

		if data != nil {
			if UT_TestDocumentSyncer {
				batchGroup = append(batchGroup, data)
			} else {
				switch v := data.(type) {
				case protocal.RawData:
					if v.Size > 0 {
						batchGroup = append(batchGroup, v.Data)
						batchGroupSize += v.Size
					}
				case map[string]*dynamodb.AttributeValue:
					batchGroup = append(batchGroup, v)
					// meaningless batchGroupSize
				}
			}
		}

		if exit || timeout || len(batchGroup) >= batchNumber || batchGroupSize >= batchSize {
			StartT = time.Now()
			batchGroupLen := len(batchGroup)
			if len(batchGroup) != 0 {
				if err := ds.write(batchGroup); err != nil {
					LOG.Crashf("%s write data failed[%v]", ds.String(), err)
				}

				batchGroup = make([]interface{}, 0, batchNumber)
				batchGroupSize = 0
			}
			writeDestDBDuration := time.Since(StartT)
			LOG.Info("%s write db batch[%v] parserChan.len[%v] readParserChanTime[%v] writeDestDbTime[%v]",
				ds.String(), batchGroupLen, len(ds.inputChan), readParserChanDuration, writeDestDBDuration)

			if exit {
				break
			}
			timeout = false
		}
	}

	LOG.Info("%s finish writing", ds.String())
}

func (ds *documentSyncer) write(input []interface{}) error {
	LOG.Debug("%s writing data with length[%v]", ds.String(), len(input))
	if len(input) == 0 {
		return nil
	}

	if UT_TestDocumentSyncer {
		UT_TestDocumentSyncer_Chan <- input
		return nil
	}

	defer atomic.AddUint64(&ds.collectionMetric.FinishCount, uint64(len(input)))
	return ds.writer.WriteBulk(input)
}
