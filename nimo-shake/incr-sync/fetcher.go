package incr_sync

import (
	"time"

	"nimo-shake/checkpoint"
	utils "nimo-shake/common"
	conf "nimo-shake/configure"
	"nimo-shake/qps"

	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	LOG "github.com/vinllen/log4go"
)

const (
	FetcherInterval = 60 // seconds
)

type Fetcher struct {
	dynamoClient *dynamodbstreams.DynamoDBStreams
	table        string
	stream       *dynamodbstreams.Stream
	shardChan    chan *utils.ShardNode
	ckptWriter   checkpoint.Writer
	metric       *utils.ReplicationMetric
}

func NewFetcher(table string, stream *dynamodbstreams.Stream, shardChan chan *utils.ShardNode, ckptWriter checkpoint.Writer, metric *utils.ReplicationMetric) *Fetcher {
	// create dynamo stream client
	dynamoStreamSession, err := utils.CreateDynamoStreamSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Error("create dynamodb stream session failed[%v]", err)
		return nil
	}

	return &Fetcher{
		dynamoClient: dynamoStreamSession,
		table:        table,
		stream:       stream,
		shardChan:    shardChan,
		ckptWriter:   ckptWriter,
		metric:       metric,
	}
}

func (f *Fetcher) Run() {
	md5Map := make(map[string]uint64)
	tableEpoch := make(map[string]int) // GlobalFetcherMoreFlag, restore previous epoch

	qos := qps.StartQoS(10)
	defer qos.Close()

	for range time.NewTicker(FetcherInterval * time.Second).C {
		shardList := make([]*utils.ShardNode, 0)
		// LOG.Debug("fetch table[%v] stream", table)

		preEpoch, ok := tableEpoch[f.table]
		if !ok {
			tableEpoch[f.table] = 0
		}

		var allShards []*dynamodbstreams.Shard
		var lastShardIdString *string = nil
		for {
			var describeStreamInput *dynamodbstreams.DescribeStreamInput
			if lastShardIdString != nil {
				describeStreamInput = &dynamodbstreams.DescribeStreamInput{
					StreamArn:             f.stream.StreamArn,
					ExclusiveStartShardId: lastShardIdString,
				}
			} else {
				describeStreamInput = &dynamodbstreams.DescribeStreamInput{
					StreamArn: f.stream.StreamArn,
				}
			}

			// limit qos of api DescribeStreamInput
			<-qos.Bucket

			desStream, err := f.dynamoClient.DescribeStream(describeStreamInput)
			if err != nil {
				LOG.Crashf("describe table[%v] with stream[%v] failed[%v]", f.table, *f.stream.StreamArn, err)
			}
			if *desStream.StreamDescription.StreamStatus == "DISABLED" {
				LOG.Crashf("table[%v] with stream[%v] has already been disabled", f.table, *f.stream.StreamArn)
			}

			allShards = append(allShards, desStream.StreamDescription.Shards...)

			if desStream.StreamDescription.LastEvaluatedShardId == nil {
				break
			} else {
				lastShardIdString = desStream.StreamDescription.LastEvaluatedShardId
				LOG.Info("table[%v] have next shardId,LastEvaluatedShardId[%v]",
					f.table, *desStream.StreamDescription.LastEvaluatedShardId)
			}
		}
		LOG.Info("fetch.Run table[%v] allShards(len:%d)[%v]", f.table, len(allShards), allShards)

		rootNode := utils.BuildShardTree(allShards, f.table, *f.stream.StreamArn)
		md5 := utils.CalMd5(rootNode)

		GlobalFetcherLock.Lock()
		curEpoch := GlobalFetcherMoreFlag[f.table]
		GlobalFetcherLock.Unlock()

		if val, ok := md5Map[f.table]; !ok || val != md5 {
			// shards is changed
			LOG.Info("table[%v] md5 changed from old[%v] to new[%v], need fetch shard", f.table, val, md5)
			md5Map[f.table] = md5
		} else if preEpoch != curEpoch {
			// old shard has already been finished
			LOG.Info("table[%v] curEpoch[%v] != preEpoch[%v]", f.table, curEpoch, preEpoch)
			tableEpoch[f.table] = curEpoch
		} else {
			LOG.Info("table[%v] md5-old[%v] md5-new[%v]", f.table, val, md5)

			continue
		}

		// extract checkpoint from mongodb
		ckptSingleMap, err := f.ckptWriter.ExtractSingleCheckpoint(f.table)
		if err != nil {
			LOG.Crashf("extract checkpoint failed[%v]", err)
		} else {
			LOG.Info("table:[%v] ckptSingleMap:[%v]", f.table, ckptSingleMap)
		}

		if tree, err := utils.PrintShardTree(rootNode); err != nil {
			LOG.Info("table[%v] traverse to print tree failed[%v]", f.table, err)
		} else {
			LOG.Info("traverse stream tree for table[%v](father->child): \n-----\n%v\n-----", f.table, tree)
		}

		// traverse shards
		err = utils.TraverseShard(rootNode, func(node *utils.ShardNode) error {
			LOG.Info("traverse shard[%v]", *node.Shard.ShardId)
			id := *node.Shard.ShardId
			var father string
			if node.Shard.ParentShardId != nil {
				father = *node.Shard.ParentShardId
			}

			ckpt, ok := ckptSingleMap[id]
			if !ok {
				// insert checkpoint
				newCkpt := &checkpoint.Checkpoint{
					ShardId:        id,
					SequenceNumber: *node.Shard.SequenceNumberRange.StartingSequenceNumber,
					Status:         checkpoint.StatusPrepareProcess,
					WorkerId:       "unknown",
					FatherId:       father,
					IteratorType:   checkpoint.IteratorTypeTrimHorizon,
					UpdateDate:     "", // empty at first
				}
				f.ckptWriter.Insert(newCkpt, f.table)
				shardList = append(shardList, node)
				LOG.Info("insert new checkpoint: %v ckptSingleMap[id]:%v", *newCkpt, ckptSingleMap[id])
				return utils.StopTraverseSonErr
			}
			switch ckpt.Status {
			case checkpoint.StatusNoNeedProcess:
				LOG.Info("no need to process: %v", *ckpt)
				return nil
			case checkpoint.StatusPrepareProcess:
				LOG.Info("status already in prepare: %v", *ckpt)
				shardList = append(shardList, node)
				return utils.StopTraverseSonErr
			case checkpoint.StatusInProcessing:
				LOG.Info("status already in processing: %v", *ckpt)
				shardList = append(shardList, node)
				return utils.StopTraverseSonErr
			case checkpoint.StatusNotProcess:
				fallthrough
			case checkpoint.StatusWaitFather:
				LOG.Info("status need to process: %v", *ckpt)
				ckpt.SequenceNumber = *node.Shard.SequenceNumberRange.StartingSequenceNumber
				ckpt.Status = checkpoint.StatusPrepareProcess
				ckpt.IteratorType = checkpoint.IteratorTypeTrimHorizon
				f.ckptWriter.Update(ckpt.ShardId, ckpt, f.table)
				shardList = append(shardList, node)
				return utils.StopTraverseSonErr
			case checkpoint.StatusDone:
				LOG.Info("already done: %v", *ckpt)
				return nil
			default:
				return fmt.Errorf("unknown checkpoint status[%v]", ckpt.Status)
			}

			return nil
		})
		if err != nil {
			LOG.Crashf("traverse shard tree failed[%v]", err)
		}

		// dispatch shard list
		for _, shard := range shardList {
			LOG.Info("need to dispatch shard[%v]", *shard.Shard.ShardId)
			f.shardChan <- shard
		}
	}
	LOG.Crashf("can't see me!")
}
