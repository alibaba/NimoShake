package incr_sync

import (
	"time"

	"nimo-shake/checkpoint"
	"nimo-shake/common"
	"nimo-shake/configure"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"fmt"
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
}

func NewFetcher(table string, stream *dynamodbstreams.Stream, shardChan chan *utils.ShardNode, ckptWriter checkpoint.Writer) *Fetcher {
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
	}
}

func (f *Fetcher) Run() {
	md5Map := make(map[string]uint64)
	tableEpoch := make(map[string]int) // GlobalFetcherMoreFlag, restore previous epoch
	for range time.NewTicker(FetcherInterval * time.Second).C {
		shardList := make([]*utils.ShardNode, 0)
		// LOG.Debug("fetch table[%v] stream", table)

		preEpoch, ok := tableEpoch[f.table]
		if !ok {
			tableEpoch[f.table] = 0
		}

		desStream, err := f.dynamoClient.DescribeStream(&dynamodbstreams.DescribeStreamInput{
			StreamArn: f.stream.StreamArn,
		})
		if err != nil {
			LOG.Crashf("describe table[%v] with stream[%v] failed[%v]", f.table, *f.stream.StreamArn, err)
		}
		if *desStream.StreamDescription.StreamStatus == "DISABLED" {
			LOG.Crashf("table[%v] with stream[%v] has already been disabled", f.table, *f.stream.StreamArn)
		}

		rootNode := utils.BuildShardTree(desStream.StreamDescription.Shards, f.table, *f.stream.StreamArn)
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
			continue
		}

		// extract checkpoint from mongodb
		ckptSingleMap, err := f.ckptWriter.ExtractSingleCheckpoint(f.table)
		if err != nil {
			LOG.Crashf("extract checkpoint failed[%v]", err)
		}

		if tree, err := utils.PrintShardTree(rootNode); err != nil {
			LOG.Info("table[%v] traverse to print tree failed[%v]", f.table, err)
		} else {
			LOG.Info("traverse stream tree for table[%v]: \n-----\n%v\n-----", f.table, tree)
		}

		// traverse shards
		err = utils.TraverseShard(rootNode, func(node *utils.ShardNode) error {
			LOG.Debug("traverse shard[%v]", *node.Shard.ShardId)
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
					IteratorType:   checkpoint.IteratorTypeSequence,
					UpdateDate:     "", // empty at first
				}
				f.ckptWriter.Insert(newCkpt, f.table)
				shardList = append(shardList, node)
				LOG.Info("insert new checkpoint: %v", *newCkpt)
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
				ckpt.IteratorType = checkpoint.IteratorTypeSequence
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
