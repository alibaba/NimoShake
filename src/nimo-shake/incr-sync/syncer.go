package incr_sync

import (
	"sync"
	"time"
	"fmt"

	"nimo-shake/protocal"
	"nimo-shake/qps"
	"nimo-shake/configure"
	"nimo-shake/common"
	"nimo-shake/checkpoint"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/aws"
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
	BatcherNumber = 1024
	BatcherSize   = 2 * utils.MB
)

func Start(streamMap map[string]*dynamodbstreams.Stream) {
	for table, stream := range streamMap {
		LOG.Info("table[%v] stream[%v] begin", table, *stream.StreamArn)

		shardChan := make(chan *utils.ShardNode, ShardChanSize)
		fetcher := NewFetcher(table, stream, shardChan)
		if fetcher == nil {
			LOG.Crashf("table[%v] stream[%v] start fetcher failed", table, *stream.StreamArn)
		}

		go fetcher.Run()

		for i := 0; i < int(conf.Options.IncreaseConcurrency); i++ {
			go func(id int) {
				for {
					shard := <-shardChan
					LOG.Info("dispatch id[%v] starts shard[%v]", id, *shard.Shard.ShardId)

					// check whether current shard is running or finished
					GlobalShardLock.Lock()
					flag := GlobalShardMap[*shard.Shard.ShardId]
					GlobalShardLock.Unlock()
					switch flag {
					case 0:
						LOG.Info("dispatch id[%v] shard[%v] isn't running, need to run", id, *shard.Shard.ShardId)
					case 1:
						LOG.Warn("dispatch id[%v] shard[%v] is running, no need to run again", id, *shard.Shard.ShardId)
						continue
					case 2:
						LOG.Warn("dispatch id[%v] shard[%v] is finished, no need to run again", id, *shard.Shard.ShardId)
						continue
					}

					// set running flag
					GlobalShardLock.Lock()
					GlobalShardMap[*shard.Shard.ShardId] = 1
					GlobalShardLock.Unlock()

					d := NewDispatcher(id, shard)
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
			}(i)
		}
	}

	select {}
}

/*-----------------------------------------------------------*/
// 1 dispatcher corresponding to 1 shard
type Dispatcher struct {
	id                  int
	shard               *utils.ShardNode
	dynamoStreamSession *dynamodbstreams.DynamoDBStreams
	targetClient        *utils.MongoConn
	ckptClient          *utils.MongoConn
	batchChan           chan *dynamodbstreams.Record
	executorChan        chan *ExecuteNode
	converter           protocal.Converter
	ns                  utils.NS
	checkpointPosition  string
	shardIt             string // only used when checkpoint is empty
	unitTestStr         string // used for UT only
	close               bool   // is close?
}

func NewDispatcher(id int, shard *utils.ShardNode) *Dispatcher {
	// create dynamo stream client
	dynamoStreamSession, err := utils.CreateDynamoStreamSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Crashf("create dynamodb stream session failed[%v]", err)
		return nil
	}

	targetClient, err := utils.NewMongoConn(conf.Options.TargetAddress, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Crashf("connect target mongodb[%v] failed[%v]", conf.Options.TargetAddress, err)
		return nil
	}

	ckptClient, err := utils.NewMongoConn(conf.Options.CheckpointAddress, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Crashf("connect checkpoint mongodb[%v] failed[%v]", conf.Options.CheckpointAddress, err)
		return nil
	}

	// converter
	converter := protocal.NewConverter(conf.Options.ConvertType)
	if converter == nil {
		LOG.Crashf("create converter[%v] failed", conf.Options.ConvertType)
	}

	d := &Dispatcher{
		id:                  id,
		shard:               shard,
		dynamoStreamSession: dynamoStreamSession,
		targetClient:        targetClient,
		ckptClient:          ckptClient,
		batchChan:           make(chan *dynamodbstreams.Record, DispatcherBatcherChanSize),
		executorChan:        make(chan *ExecuteNode, DispatcherExecuterChanSize),
		converter:           converter,
		ns: utils.NS{
			Database:   conf.Options.Id,
			Collection: shard.Table,
		},
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

	// check father finished
	for {
		fatherCkpt, err := checkpoint.QueryCkpt(father, d.ckptClient, conf.Options.CheckpointDb, d.ns.Collection)
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
	LOG.Info("%s father shard[%v] finished", d.String(), father)

	// fetch shardIt
	shardIt, ok := checkpoint.GlobalShardIteratorMap.Get(*d.shard.Shard.ShardId)
	if ok {
		checkpoint.GlobalShardIteratorMap.Delete(*d.shard.Shard.ShardId)

		LOG.Info("%s current shard already in ShardIteratorMap", d.String())
	} else {
		// check current checkpoint
		ckpt, err := checkpoint.QueryCkpt(*d.shard.Shard.ShardId, d.ckptClient, conf.Options.CheckpointDb, d.ns.Collection)
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
			} else {
				// dynamodb rule: this is only used when restart in 30 minutes
				shardIt = ckpt.ShardIt
			}
		} else {
			shardItOut, err := d.dynamoStreamSession.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
				ShardId: d.shard.Shard.ShardId,
				// SequenceNumber:    d.shard.Shard.SequenceNumberRange.StartingSequenceNumber,
				SequenceNumber:    aws.String(ckpt.SequenceNumber),
				ShardIteratorType: aws.String(checkpoint.IteratorTypeSequence),
				StreamArn:         aws.String(d.shard.ShardArn),
			})
			if err != nil {
				LOG.Crashf("%s get shard iterator failed[%v]", d.String(), err)
			}
			shardIt = *shardItOut.ShardIterator
		}
	}

	LOG.Info("%s start with shard iterator[%v]", d.String(), shardIt)

	// update checkpoint: in-processing
	err := checkpoint.UpdateCkptSet(*d.shard.Shard.ShardId, bson.M{
		checkpoint.FieldStatus: checkpoint.StatusInProcessing,
	}, d.ckptClient, conf.Options.CheckpointDb, d.ns.Collection)
	if err != nil {
		LOG.Crashf("%s update checkpoint to in-processing failed[%v]", d.String(), err)
	}
	LOG.Info("%s shard-id[%v] finish updating checkpoint", d.String(), shardIt)

	// get records
	d.getRecords(shardIt)
	LOG.Info("%s finish shard", d.String())

	// update checkpoint: finish
	err = checkpoint.UpdateCkptSet(*d.shard.Shard.ShardId, bson.M{
		checkpoint.FieldStatus: checkpoint.StatusDone,
	}, d.ckptClient, conf.Options.CheckpointDb, d.ns.Collection)
	if err != nil {
		LOG.Crashf("%s update checkpoint to done failed[%v]", d.String(), err)
	}

	d.close = true
	LOG.Info("%s close", d.String())
}

func (d *Dispatcher) getRecords(shardIt string) {
	qos := qps.StartQoS(int(conf.Options.QpsFull))
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
	tp                 string
	operate            []interface{}
	lastSequenceNumber string
}

func (d *Dispatcher) batcher() {
	node := &ExecuteNode{
		operate: make([]interface{}, 0, BatcherNumber),
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
		}

		if !ok || timeout {
			if len(node.operate) != 0 {
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
			if len(node.operate) != 0 {
				// preEvent != ""
				d.executorChan <- node
			}

			node = &ExecuteNode{
				tp:      *record.EventName,
				operate: make([]interface{}, 0, BatcherNumber),
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

		// LOG.Info("cccc1 %v", index.Data)

		switch *record.EventName {
		case EventInsert:
			value, err := d.converter.Run(record.Dynamodb.NewImage)
			if err != nil {
				LOG.Crashf("%s converter do insert meets error[%v]", d.String(), err)
			}

			node.operate = append(node.operate, value.Data)
		case EventMODIFY:
			value, err := d.converter.Run(record.Dynamodb.NewImage)
			if err != nil {
				LOG.Crashf("%s converter do insert meets error[%v]", d.String(), err)
			}

			node.operate = append(node.operate, index.Data, value.Data)
		case EventRemove:
			node.operate = append(node.operate, index.Data)
		default:
			LOG.Crashf("%s unknown event name[%v]", d.String(), *record.EventName)
		}

		node.lastSequenceNumber = *record.Dynamodb.SequenceNumber
		batchNr += 1
		batchSize += index.Size
	}

	LOG.Info("%s batcher exit", d.String())
	close(d.executorChan)
}

func (d *Dispatcher) executor() {
	for node := range d.executorChan {
		LOG.Info("%s try write data with length[%v]", d.String(), len(node.operate))
		for {
			LOG.Debug("%s operate[%v] write data[%v]", d.String(), node.tp, node.operate)
			// LOG.Info("dddd %s operate[%v] write data[%v]", d.String(), node.tp, node.operate)
			bulk := d.targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Bulk()
			switch node.tp {
			case EventInsert:
				bulk.Insert(node.operate...)
			case EventMODIFY:
				bulk.Update(node.operate...)
			case EventRemove:
				bulk.Remove(node.operate...)
			}

			if _, err := bulk.Run(); err != nil {
				index, errMsg, dup := utils.FindFirstErrorIndexAndMessage(err.Error())
				if index == -1 || !dup {
					LOG.Crashf("%s bulk[%v] run[%v] failed[%v], index[%v] dup[%v]", d.String(), node.tp,
						node.operate, err, index, dup)
				}
				LOG.Warn("%s bulk[%v] run[%v] meets dup[%v], skip and retry", d.String(), node.tp, node.operate, errMsg)

				// re-run the remains data if not empty
				nextIndex := index + 1
				if node.tp == EventMODIFY {
					nextIndex = index + 2
				}
				if nextIndex < len(node.operate) {
					node.operate = node.operate[nextIndex:]
					continue
				}
			}

			break
		}
		d.checkpointPosition = node.lastSequenceNumber
	}

	LOG.Info("%s executor exit", d.String())
}

// used to set checkpoint
func (d *Dispatcher) ckptManager() {
	var prevCkptPosition string

	initCkpt, err := checkpoint.QueryCkpt(*d.shard.Shard.ShardId, d.ckptClient, conf.Options.CheckpointDb, d.ns.Collection)
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
					checkpoint.FieldShardIt:   d.shardIt,
					checkpoint.FieldTimestamp: time.Now().Format(utils.GolangSecurityTime),
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

			ckpt = bson.M{
				checkpoint.FieldSeqNum:       d.checkpointPosition,
				checkpoint.FieldIteratorType: checkpoint.IteratorTypeSequence,
				checkpoint.FieldTimestamp:    time.Now().Format(utils.GolangSecurityTime),
			}
		}

		prevCkptPosition = d.checkpointPosition
		err := checkpoint.UpdateCkptSet(*d.shard.Shard.ShardId, ckpt, d.ckptClient, conf.Options.CheckpointDb, d.ns.Collection)
		if err != nil {
			LOG.Error("%s update table[%v] shard[%v] input[%v] failed[%v]", d.String(), d.ns.Collection,
				*d.shard.Shard.ShardId, ckpt, err)
		} else {
			LOG.Info("%s update table[%v] shard[%v] input[%v] ok", d.String(), d.ns.Collection,
				*d.shard.Shard.ShardId, ckpt)
		}
	}
}
