package incr_sync

import (
	"fmt"
	"sync"
	"time"

	"github.com/alibaba/NimoShake/src/nimo-shake/checkpoint"
	utils "github.com/alibaba/NimoShake/src/nimo-shake/common"
	conf "github.com/alibaba/NimoShake/src/nimo-shake/configure"
	"github.com/alibaba/NimoShake/src/nimo-shake/protocal"
	"github.com/alibaba/NimoShake/src/nimo-shake/qps"
	"github.com/alibaba/NimoShake/src/nimo-shake/writer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const (
	ShardChanSize            = 4096
	WaitFatherFinishInterval = 20 // seconds
	GetRecordsInterval       = 3
	CheckpointFlushInterval  = 20

	DispatcherBatcherChanSize  = 4096
	DispatcherExecuterChanSize = 4096

	IncrBatcherTimeout = 1

	EventInsert = "INSERT"
	EventMODIFY = "MODIFY"
	EventRemove = "REMOVE"
)

var (
	GlobalShardMap  = make(map[string]int) // 1: running 2: finish
	GlobalShardLock sync.Mutex

	GlobalFetcherMoreFlag = make(map[string]int) // epoch of table
	GlobalFetcherLock     sync.Mutex

	// move from const to var, used for UT
	BatcherNumber = 25
	BatcherSize   = 2 * utils.MB

	// incr sync metric
	replMetric  *utils.ReplicationMetric
	ckptWriterG checkpoint.Writer
)

type IncrSycnMetric struct {
	recordsGet   uint64
	recordsWrite uint64
}

func Start(streamMap map[string]*dynamodbstreams.Stream, ckptWriter checkpoint.Writer) {
	replMetric = utils.NewMetric(utils.TypeIncr, utils.METRIC_CKPT_TIMES|utils.METRIC_SUCCESS|utils.METRIC_TPS)
	ckptWriterG = ckptWriter
	for table, stream := range streamMap {
		LOG.Info("table[%v] stream[%v] begin", table, *stream.StreamArn)

		shardChan := make(chan *utils.ShardNode, ShardChanSize)
		fetcher := NewFetcher(table, stream, shardChan, ckptWriter, replMetric)
		if fetcher == nil {
			LOG.Crashf("table[%v] stream[%v] start fetcher failed", table, *stream.StreamArn)
		}

		go fetcher.Run()

		for i := 0; i < int(conf.Options.IncreaseConcurrency); i++ {
			go func(id int, table string) {
				for {
					shard := <-shardChan
					LOG.Info("table[%s] dispatch id[%v] starts shard[%v]", table, id, *shard.Shard.ShardId)

					// check whether current shard is running or finished
					GlobalShardLock.Lock()
					flag := GlobalShardMap[*shard.Shard.ShardId]
					GlobalShardLock.Unlock()
					switch flag {
					case 0:
						LOG.Info("table[%s] dispatch id[%v] shard[%v] isn't running, need to run",
							table, id, *shard.Shard.ShardId)
					case 1:
						LOG.Warn("table[%s] dispatch id[%v] shard[%v] is running, no need to run again",
							table, id, *shard.Shard.ShardId)
						continue
					case 2:
						LOG.Warn("table[%s] dispatch id[%v] shard[%v] is finished, no need to run again",
							table, id, *shard.Shard.ShardId)
						continue
					}

					// set running flag
					GlobalShardLock.Lock()
					GlobalShardMap[*shard.Shard.ShardId] = 1
					GlobalShardLock.Unlock()

					d := NewDispatcher(id, shard, ckptWriter, replMetric)
					d.Run()

					// set finished flag
					GlobalShardLock.Lock()
					GlobalShardMap[*shard.Shard.ShardId] = 2
					GlobalShardLock.Unlock()

					// update table epoch
					GlobalFetcherLock.Lock()
					GlobalFetcherMoreFlag[shard.Table] += 1
					GlobalFetcherLock.Unlock()

					LOG.Info("dispatch id[%v] finishes shard[%v]", id, *shard.Shard.ShardId)
				}
			}(i, table)
		}
	}

	select {}
}

/*-----------------------------------------------------------*/
// 1 dispatcher corresponding to 1 shard
type Dispatcher struct {
	id                        int
	shard                     *utils.ShardNode
	table                     string
	dynamoStreamSession       *dynamodbstreams.DynamoDBStreams
	targetWriter              writer.Writer
	batchChan                 chan *dynamodbstreams.Record
	executorChan              chan *ExecuteNode
	converter                 protocal.Converter
	ns                        utils.NS
	checkpointPosition        string
	checkpointApproximateTime string
	shardIt                   string // only used when checkpoint is empty
	unitTestStr               string // used for UT only
	close                     bool   // is close?
	ckptWriter                checkpoint.Writer
	metric                    *utils.ReplicationMetric
}

func NewDispatcher(id int, shard *utils.ShardNode, ckptWriter checkpoint.Writer, metric *utils.ReplicationMetric) *Dispatcher {
	// create dynamo stream client
	dynamoStreamSession, err := utils.CreateDynamoStreamSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Crashf("table[%s] create dynamodb stream session failed[%v]", shard.Table, err)
		return nil
	}

	ns := utils.NS{
		Database:   conf.Options.Id,
		Collection: shard.Table,
	}

	// create target writer
	targetWriter := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, ns, conf.Options.LogLevel)
	if targetWriter == nil {
		LOG.Crashf("table[%s] create target-writer with type[%v] and address[%v] failed", ns.Collection,
			conf.Options.TargetType, conf.Options.TargetAddress)
	}

	// converter
	converter := protocal.NewConverter(conf.Options.ConvertType)
	if converter == nil {
		LOG.Crashf("table[%s] create converter[%v] failed", conf.Options.ConvertType)
	}

	d := &Dispatcher{
		id:                  id,
		shard:               shard,
		dynamoStreamSession: dynamoStreamSession,
		targetWriter:        targetWriter,
		batchChan:           make(chan *dynamodbstreams.Record, DispatcherBatcherChanSize),
		executorChan:        make(chan *ExecuteNode, DispatcherExecuterChanSize),
		converter:           converter,
		ns:                  ns,
		ckptWriter:          ckptWriter,
		metric:              metric,
	}

	go d.batcher()
	go d.executor()
	go d.ckptManager()

	return d
}

func (d *Dispatcher) String() string {
	if d.unitTestStr != "" {
		return d.unitTestStr
	}
	return fmt.Sprintf("dispatcher[%v] table[%v] shard-id[%v]", d.id, d.ns.Collection, *d.shard.Shard.ShardId)
}

func (d *Dispatcher) Run() {
	// re-check father shard finished
	var father string
	if d.shard.Shard.ParentShardId != nil {
		father = *d.shard.Shard.ParentShardId
	}

	LOG.Info("%s begins, check father shard[%v] status", d.String(), father)

	if father != "" {
		// check father finished
		for {
			fatherCkpt, err := d.ckptWriter.Query(father, d.ns.Collection)
			if err != nil && err.Error() != utils.NotFountErr {
				LOG.Crashf("%s query father[%v] checkpoint fail[%v]", d.String(), father, err)
			}

			// err != nil means utils.NotFountErr
			if err != nil || !checkpoint.IsStatusProcessing(fatherCkpt.Status) {
				break
			}

			LOG.Warn("%s father shard[%v] is still running, waiting...", d.String(), father)
			time.Sleep(WaitFatherFinishInterval * time.Second)
		}
	}

	LOG.Info("%s father shard[%v] finished", d.String(), father)

	// fetch shardIt
	shardIt, ok := checkpoint.GlobalShardIteratorMap.Get(*d.shard.Shard.ShardId)
	if ok {
		checkpoint.GlobalShardIteratorMap.Delete(*d.shard.Shard.ShardId)

		LOG.Info("%s current shard already in ShardIteratorMap", d.String())
	} else {
		// check current checkpoint
		ckpt, err := d.ckptWriter.Query(*d.shard.Shard.ShardId, d.ns.Collection)
		if err != nil {
			LOG.Crashf("%s query current[%v] checkpoint fail[%v]", d.String(), *d.shard.Shard.ShardId, err)
		}
		if ckpt.IteratorType == checkpoint.IteratorTypeLatest && checkpoint.IsStatusProcessing(ckpt.Status) {
			if ckpt.ShardIt == "" {
				/*
				 * iterator_type == "LATEST" means this shard has been found before full-sync.
				 * When checkpoint updated, this field will be updated to "AT_SEQUENCE_NUMBER" in incr_sync stage,
				 * so this case only happened when nimo-shake finished full-sync and then crashed before incr_sync
				 */
				LOG.Crashf("%s shard[%v] iterator type[%v] abnormal, status[%v], need full sync", d.String(),
					*d.shard.Shard.ShardId, ckpt.IteratorType, ckpt.Status)
			} else if ckpt.ShardIt == checkpoint.InitShardIt {
				// means generate new shardIt
				shardItOut, err := d.dynamoStreamSession.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
					ShardId:           d.shard.Shard.ShardId,
					SequenceNumber:    aws.String(ckpt.SequenceNumber),
					ShardIteratorType: aws.String(checkpoint.IteratorTypeAtSequence),
					StreamArn:         aws.String(d.shard.ShardArn),
				})
				if err != nil {
					LOG.Crashf("%s get shard iterator[SequenceNumber:%v, ShardIteratorType:%s, StreamArn:%s] "+
						"failed[%v]", d.String(), ckpt.SequenceNumber, checkpoint.IteratorTypeAtSequence,
						d.shard.ShardArn, err)
				}
				shardIt = *shardItOut.ShardIterator
			} else {
				// dynamodb rule: this is only used when restart in 30 minutes
				shardIt = ckpt.ShardIt
			}
		} else {
			shardItOut, err := d.dynamoStreamSession.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
				ShardId:           d.shard.Shard.ShardId,
				SequenceNumber:    aws.String(ckpt.SequenceNumber),
				ShardIteratorType: aws.String(checkpoint.IteratorTypeAfterSequence),
				StreamArn:         aws.String(d.shard.ShardArn),
			})
			if err != nil {
				LOG.Crashf("%s get shard iterator[SequenceNumber:%v, ShardIteratorType:%s, StreamArn:%s] "+
					"failed[%v]", d.String(), ckpt.SequenceNumber, checkpoint.IteratorTypeAtSequence,
					d.shard.ShardArn, err)
			}
			shardIt = *shardItOut.ShardIterator
		}
	}

	LOG.Info("%s start with shard iterator[%v]", d.String(), shardIt)

	// update checkpoint: in-processing
	err := d.ckptWriter.UpdateWithSet(*d.shard.Shard.ShardId, map[string]interface{}{
		checkpoint.FieldStatus: checkpoint.StatusInProcessing,
	}, d.ns.Collection)
	if err != nil {
		LOG.Crashf("%s update checkpoint to in-processing failed[%v]", d.String(), err)
	}
	LOG.Info("%s shard-id[%v] finish updating checkpoint", d.String(), shardIt)

	// get records
	d.getRecords(shardIt)
	LOG.Info("%s finish shard", d.String())

	// update checkpoint: finish
	err = d.ckptWriter.UpdateWithSet(*d.shard.Shard.ShardId, map[string]interface{}{
		checkpoint.FieldStatus: checkpoint.StatusDone,
	}, d.ns.Collection)
	if err != nil {
		LOG.Crashf("%s update checkpoint to done failed[%v]", d.String(), err)
	}

	d.close = true
	LOG.Info("%s close", d.String())
}

func (d *Dispatcher) getRecords(shardIt string) {
	qos := qps.StartQoS(int(conf.Options.QpsIncr))
	defer qos.Close()

	next := &shardIt
	for {
		<-qos.Bucket

		// LOG.Info("%s bbbb0 ", d.String())

		records, err := d.dynamoStreamSession.GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: next,
			Limit:         aws.Int64(conf.Options.QpsIncrBatchNum),
		})
		if err != nil {
			LOG.Crashf("%s get records with iterator[%v] failed[%v]", d.String(), *next, err)
		}

		// LOG.Info("%s bbbb1 %v", d.String(), *next)

		next = records.NextShardIterator

		if len(records.Records) == 0 && next != nil {
			d.shardIt = *next // update shardIt
			time.Sleep(GetRecordsInterval * time.Second)
			continue
		}

		d.metric.AddGet(uint64(len(records.Records)))

		// LOG.Info("bbbb2 ", records.Records)

		// do write
		for _, record := range records.Records {
			d.batchChan <- record
		}

		if next == nil {
			break
		}
	}

	close(d.batchChan)
	LOG.Info("%s getRecords exit", d.String())
}

type ExecuteNode struct {
	tp                          string
	operate                     []interface{}
	index                       []interface{}
	lastSequenceNumber          string
	approximateCreationDateTime string
}

func (d *Dispatcher) batcher() {
	node := &ExecuteNode{
		operate: make([]interface{}, 0, BatcherNumber),
		index:   make([]interface{}, 0, BatcherNumber),
	}

	var preEvent string
	var batchNr int
	var batchSize int
	for {
		var record *dynamodbstreams.Record
		ok := true
		timeout := false

		select {
		case record, ok = <-d.batchChan:
		case <-time.After(time.Second * IncrBatcherTimeout):
			timeout = true
			record = nil
		}

		if !ok || timeout {
			if len(node.operate) != 0 || len(node.index) != 0 {
				d.executorChan <- node
				node = &ExecuteNode{
					tp:      "",
					operate: make([]interface{}, 0, BatcherNumber),
				}
				preEvent = ""
				batchNr = 0
				batchSize = 0
			}
			if !ok {
				// channel close
				break
			}
			// timeout
			continue
		}

		if *record.EventName != preEvent || batchNr >= BatcherNumber || batchSize >= BatcherSize {
			// need split
			if len(node.operate) != 0 || len(node.index) != 0 {
				// preEvent != ""
				d.executorChan <- node
			}

			node = &ExecuteNode{
				tp:      *record.EventName,
				operate: make([]interface{}, 0, BatcherNumber), // need fetch data field when type is RawData
				index:   make([]interface{}, 0, BatcherNumber), // index list
			}
			preEvent = *record.EventName
			batchNr = 0
			batchSize = 0
		}

		// parse index
		index, err := d.converter.Run(record.Dynamodb.Keys)
		if err != nil {
			LOG.Crashf("%s convert parse[%v] failed[%v]", d.String(), record.Dynamodb.Keys, err)
		}

		// LOG.Info("~~~op[%v] data: %v", *record.EventName, record)

		// batch into list
		switch *record.EventName {
		case EventInsert:
			value, err := d.converter.Run(record.Dynamodb.NewImage)
			if err != nil {
				LOG.Crashf("%s converter do insert meets error[%v]", d.String(), err)
			}

			switch d.targetWriter.(type) {
			case *writer.MongoWriter:
				node.operate = append(node.operate, value.(protocal.RawData).Data)
				node.index = append(node.index, index.(protocal.RawData).Data)
			case *writer.DynamoProxyWriter:
				node.operate = append(node.operate, value)
				node.index = append(node.index, index)
			default:
				LOG.Crashf("unknown operator")
			}
		case EventMODIFY:
			value, err := d.converter.Run(record.Dynamodb.NewImage)
			if err != nil {
				LOG.Crashf("%s converter do insert meets error[%v]", d.String(), err)
			}

			switch d.targetWriter.(type) {
			case *writer.MongoWriter:
				node.operate = append(node.operate, value.(protocal.RawData).Data)
				node.index = append(node.index, index.(protocal.RawData).Data)
			case *writer.DynamoProxyWriter:
				node.operate = append(node.operate, value)
				node.index = append(node.index, index)
			default:
				LOG.Crashf("unknown operator")
			}
		case EventRemove:
			switch d.targetWriter.(type) {
			case *writer.MongoWriter:
				node.index = append(node.index, index.(protocal.RawData).Data)
			case *writer.DynamoProxyWriter:
				node.index = append(node.index, index)
			default:
				LOG.Crashf("unknown operator")
			}
		default:
			LOG.Crashf("%s unknown event name[%v]", d.String(), *record.EventName)
		}

		node.lastSequenceNumber = *record.Dynamodb.SequenceNumber
		if record.Dynamodb.ApproximateCreationDateTime != nil {
			node.approximateCreationDateTime = record.Dynamodb.ApproximateCreationDateTime.String()
		}
		batchNr += 1
		// batchSize += index.Size
	}

	LOG.Info("%s batcher exit", d.String())
	close(d.executorChan)
}

func (d *Dispatcher) executor() {
	for node := range d.executorChan {
		LOG.Debug("%s try write data with length[%v], tp[%v] approximate[%v]", d.String(), len(node.index),
			node.tp, node.approximateCreationDateTime)
		var err error
		switch node.tp {
		case EventInsert:
			err = d.targetWriter.Insert(node.operate, node.index)
		case EventMODIFY:
			err = d.targetWriter.Update(node.operate, node.index)
		case EventRemove:
			err = d.targetWriter.Delete(node.index)
		default:
			LOG.Crashf("unknown write operation[%v]", node.tp)
		}

		if err != nil {
			LOG.Crashf("execute command[%v] failed[%v]", node.tp, err)
		}

		d.metric.AddSuccess(uint64(len(node.index)))
		d.metric.AddCheckpoint(1)
		d.checkpointPosition = node.lastSequenceNumber
		d.checkpointApproximateTime = node.approximateCreationDateTime
	}

	LOG.Info("%s executor exit", d.String())
}

// used to set checkpoint
func (d *Dispatcher) ckptManager() {
	var prevCkptPosition string

	initCkpt, err := d.ckptWriter.Query(*d.shard.Shard.ShardId, d.ns.Collection)
	if err != nil && err.Error() != utils.NotFountErr {
		LOG.Crashf("%s query checkpoint failed[%v]", d.String(), err)
	}

	for range time.NewTicker(CheckpointFlushInterval * time.Second).C {
		if d.close {
			break
		}

		var ckpt bson.M
		if d.checkpointPosition == "" {
			if d.shardIt != "" {
				// update shardIt
				ckpt = bson.M{
					checkpoint.FieldShardIt:         d.shardIt,
					checkpoint.FieldTimestamp:       time.Now().Format(utils.GolangSecurityTime),
					checkpoint.FieldApproximateTime: d.checkpointApproximateTime,
				}

			} else {
				continue
			}
		} else {
			if d.checkpointPosition == prevCkptPosition {
				continue
			}
			// do not update when checkpoint < init checkpoint
			if d.checkpointPosition < initCkpt.SequenceNumber {
				LOG.Warn("%s current checkpoint[%v] < init checkpoint[%v], no need to update", d.String(),
					d.checkpointPosition, initCkpt.SequenceNumber)
				continue
			}

			ckpt = map[string]interface{}{
				checkpoint.FieldSeqNum:          d.checkpointPosition,
				checkpoint.FieldIteratorType:    checkpoint.IteratorTypeAtSequence,
				checkpoint.FieldTimestamp:       time.Now().Format(utils.GolangSecurityTime),
				checkpoint.FieldApproximateTime: d.checkpointApproximateTime,
			}
		}

		prevCkptPosition = d.checkpointPosition
		err := d.ckptWriter.UpdateWithSet(*d.shard.Shard.ShardId, ckpt, d.ns.Collection)
		if err != nil {
			LOG.Error("%s update table[%v] shard[%v] input[%v] failed[%v]", d.String(), d.ns.Collection,
				*d.shard.Shard.ShardId, ckpt, err)
		} else {
			LOG.Info("%s update table[%v] shard[%v] input[%v] ok", d.String(), d.ns.Collection,
				*d.shard.Shard.ShardId, ckpt)
		}
	}
}

func RestAPI() {
	type IncrSyncInfo struct {
		Get         uint64      `json:"records_get"`
		Write       uint64      `json:"records_write"`
		CkptTimes   uint64      `json:"checkpoint_times"`
		UpdateTimes interface{} `json:"checkpoint_update_times"`
		Error       string      `json:"error"`
	}

	type CheckpointInfo struct {
		UpdateTime      string `json:"update_time"`
		ApproximateTime string `json:"sync_approximate_time"`
		FatherShardId   string `json:"father_shard_id"`
		SequenceNumber  string `json:sequence_number`
	}

	utils.IncrSyncHttpApi.RegisterAPI("/metric", nimo.HttpGet, func([]byte) interface{} {
		ckpt, err := ckptWriterG.ExtractCheckpoint()
		if err != nil {
			return &IncrSyncInfo{
				Error: err.Error(),
			}
		}

		retCkptMap := make(map[string]map[string]interface{}, len(ckpt))
		for table, ckptShardMap := range ckpt {
			shardMap := make(map[string]interface{}, 1)
			for shard, ckptVal := range ckptShardMap {
				if ckptVal.Status != string(utils.StatusProcessing) {
					continue
				}

				shardMap[shard] = &CheckpointInfo{
					UpdateTime:      ckptVal.UpdateDate,
					FatherShardId:   ckptVal.FatherId,
					ApproximateTime: ckptVal.ApproximateTime,
					SequenceNumber:  ckptVal.SequenceNumber,
				}
			}
			retCkptMap[table] = shardMap
		}

		return &IncrSyncInfo{
			Get:         replMetric.Get(),
			Write:       replMetric.Success(),
			CkptTimes:   replMetric.CheckpointTimes,
			UpdateTimes: retCkptMap,
		}
	})

}
