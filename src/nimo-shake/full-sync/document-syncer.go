package full_sync

import (
	"time"
	"fmt"

	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/writer"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"nimo-shake/protocal"
)

const (
	batchNumber  = 25           // dynamo-proxy limit
	batchSize    = 2 * utils.MB // mongodb limit: 16MB
	batchTimeout = 1            // seconds
)

var (
	UT_TestDocumentSyncer = false
	UT_TestDocumentSyncer_Chan chan []interface{}
)

/*------------------------------------------------------*/
// one document link corresponding to one documentSyncer
type documentSyncer struct {
	tableSyncerId int
	id            int // documentSyncer id
	ns            utils.NS
	inputChan     chan interface{} // parserChan in table-syncer
	writer        writer.Writer
}

func NewDocumentSyncer(tableSyncerId int, table string, id int, inputChan chan interface{}, tableDescribe *dynamodb.TableDescription) *documentSyncer {
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
		tableSyncerId: tableSyncerId,
		id:            id,
		inputChan:     inputChan,
		writer:        w,
		ns:            ns,
	}
}

func (ds *documentSyncer) String() string {
	return fmt.Sprintf("tableSyncer[%v] documentSyncer[%v] ns[%v]", ds.tableSyncerId, ds.id, ds.ns)
}

func (ds *documentSyncer) Close() {
	ds.writer.Close()
}

func (ds *documentSyncer) Run() {
	var data interface{}
	var ok bool
	batchGroup := make([]interface{}, 0, batchNumber)
	timeout := false
	batchGroupSize := 0
	exit := false
	for {
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
			if len(batchGroup) != 0 {
				if err := ds.write(batchGroup); err != nil {
					LOG.Crashf("%s write data failed[%v]", ds.String(), err)
				}

				batchGroup = make([]interface{}, 0, batchNumber)
				batchGroupSize = 0
			}

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

	return ds.writer.WriteBulk(input)
}
