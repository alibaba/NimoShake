package full_sync

import (
	"time"
	"fmt"

	"nimo-shake/protocal"
	"nimo-shake/common"
	"nimo-shake/configure"

	LOG "github.com/vinllen/log4go"
)

const (
	batchNumber  = 512
	batchSize    = 2 * utils.MB // mongodb limit: 16MB
	batchTimeout = 1            // seconds
)

/*------------------------------------------------------*/
// one document link corresponding to one documentSyncer
type documentSyncer struct {
	tableSyncerId int
	id            int // documentSyncer id
	ns            utils.NS
	inputChan     chan protocal.MongoData // parserChan in table-syncer
	targetConn    *utils.MongoConn
}

func NewDocumentSyncer(tableSyncerId int, table string, id int, inputChan chan protocal.MongoData) *documentSyncer {
	targetConn, err := utils.NewMongoConn(conf.Options.TargetAddress, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("tableSyncer[%v] documentSyncer[%v] create mongodb session error[%v]", tableSyncerId, id, err)
		return nil
	}

	return &documentSyncer{
		tableSyncerId: tableSyncerId,
		id:            id,
		inputChan:     inputChan,
		targetConn:    targetConn,
		ns: utils.NS{
			Database:   conf.Options.Id,
			Collection: table,
		},
	}
}

func (ds *documentSyncer) String() string {
	return fmt.Sprintf("tableSyncer[%v] documentSyncer[%v]", ds.tableSyncerId, ds.id)
}

func (ds *documentSyncer) Close() {
	ds.targetConn.Close()
}

func (ds *documentSyncer) Run() {
	var data protocal.MongoData
	var ok bool
	batchGroup := make([]protocal.MongoData, 0, batchNumber)
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
		}

		LOG.Debug("exit[%v], timeout[%v], len(batchGroup)[%v], batchGroupSize[%v], data[%v]", exit, timeout,
			len(batchGroup), batchGroupSize, data)

		if data.Size > 0 {
			batchGroup = append(batchGroup, data)
			batchGroupSize += data.Size
		}

		if exit || timeout || len(batchGroup) >= batchNumber || batchGroupSize >= batchSize {
			if len(batchGroup) != 0 {
				if err := ds.writer(batchGroup); err != nil {
					LOG.Crashf("%s write data failed[%v]", ds.String(), err)
				}

				batchGroup = make([]protocal.MongoData, 0, batchNumber)
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

func (ds *documentSyncer) writer(input []protocal.MongoData) error {
	LOG.Debug("%s writing data with length[%v]", ds.String(), len(input))
	if len(input) == 0 {
		return nil
	}

	docList := make([]interface{}, 0, len(input))
	for _, ele := range input {
		docList = append(docList, ele.Data)
	}

	if err := ds.targetConn.Session.DB(ds.ns.Database).C(ds.ns.Collection).Insert(docList...); err != nil {
		return fmt.Errorf("insert docs with length[%v] into ns[%s] of dest mongo failed[%v]. first doc: %v",
			len(docList), ds.ns, err, docList[0])
	}
	return nil
}
