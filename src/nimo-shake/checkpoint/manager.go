package checkpoint

import (
	"fmt"
	"time"

	"nimo-shake/common"
	"nimo-shake/filter"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/aws"
)

// check whether need full sync
func CheckCkpt(ckptWriter Writer, dynamoStreams *dynamodbstreams.DynamoDBStreams) (bool, map[string]*dynamodbstreams.Stream, error) {
	// fetch target checkpoint status table
	status, err := ckptWriter.FindStatus()
	if err != nil {
		return false, nil, fmt.Errorf("find checkpoint status failed[%v]", err)
	}
	// need full sync if status != CheckpointStatusValueIncrSync which means last time sync isn't incr sync
	if status != CheckpointStatusValueIncrSync {
		LOG.Info("checkpoint status[%v] != %v, need full sync", status, CheckpointStatusValueIncrSync)
		return false, nil, nil
	}

	// extract checkpoint from mongodb
	ckptMap, err := ckptWriter.ExtractCheckpoint()
	if err != nil {
		LOG.Error("extract checkpoint failed[%v]", err)
		return false, nil, err
	}
	LOG.Info("checkpoint map: %v", ckptMap)

	// fetch source stream shard information
	/*
	 * Currently, nimo-shake can't handle DDL like create table and drop table.
	 * So, if user crate or drop table and restart, nimo-shake will run full-sync.
	 * Plus, DDL can't be run when nimo-shake starts, no matter in full-sync or
	 * incr-sync.
	 */
	streamMap := make(map[string]*dynamodbstreams.Stream, len(ckptMap))
	for {
		streamList, err := dynamoStreams.ListStreams(&dynamodbstreams.ListStreamsInput{})
		if err != nil {
			fmt.Errorf("fetch dynamodb stream list failed[%v]", err)
		}

		// handle stream list, one table may have several streams
		for _, stream := range streamList.Streams {
			if _, ok := streamMap[*stream.TableName]; ok {
				continue
			}

			if filter.IsFilter(*stream.TableName) {
				LOG.Info("table[%v] filtered", *stream.TableName)
				continue
			}

			if exist, err := CheckSingleStream(stream, dynamoStreams, ckptMap); err != nil {
				return false, nil, err
			} else if exist {
				streamMap[*stream.TableName] = stream
			} else {
				// need full sync
				LOG.Warn("table[%v] not exists on checkpoint, need full sync", *stream.TableName)
				return false, nil, nil
			}
		}

		if streamList.LastEvaluatedStreamArn == nil {
			// end
			break
		}
	}

	for key := range ckptMap {
		if _, ok := streamMap[key]; !ok {
			LOG.Info("table[%v] not exist on streams", key)
			return false, nil, nil
		}
	}
	return true, streamMap, nil
}

func CheckSingleStream(stream *dynamodbstreams.Stream, dynamoStreams *dynamodbstreams.DynamoDBStreams,
	ckptMap map[string]map[string]*Checkpoint) (bool, error) {
	describeResult, err := dynamoStreams.DescribeStream(&dynamodbstreams.DescribeStreamInput{
		StreamArn: stream.StreamArn,
	})
	if err != nil {
		return false, fmt.Errorf("describe stream[%v] with table[%v] failed[%v]", stream.StreamArn,
			stream.TableName, err)
	}
	if *describeResult.StreamDescription.StreamStatus == "DISABLED" {
		// stream is disabled
		return false, nil
	}

	// check table exists on checkpoint map
	ckptInnerMap, ok := ckptMap[*stream.TableName]
	if !ok {
		LOG.Warn("collection[%v] isn't exist on checkpoint map", *stream.TableName)
		return false, nil
	}

	// convert shard list to map
	shardMap := make(map[string]*dynamodbstreams.Shard)
	for _, shard := range describeResult.StreamDescription.Shards {
		shardMap[*shard.ShardId] = shard
	}

	LOG.Info("dynamo stream shard map: %v", shardMap)

	// check shards exist
	for key, ckpt := range ckptInnerMap {
		if shard, ok := shardMap[key]; !ok {
			LOG.Warn("collection[%v] with shard[%v] isn't exist on the stream, status[%v]", *stream.TableName,
				key, ckpt.Status)
			if !IsStatusNoNeedProcess(ckpt.Status) {
				return false, nil
			}
		} else if ckpt.Status == StatusInProcessing && *shard.SequenceNumberRange.StartingSequenceNumber > ckpt.SequenceNumber &&
				ckpt.SequenceNumber != "" {
			LOG.Warn("collection[%v] with shard[%v] shard.StartingSequenceNumber[%v] > checkpoint.SequenceNumber[%v]",
				*stream.TableName, key, *shard.SequenceNumberRange.StartingSequenceNumber, ckpt.SequenceNumber)
			return false, nil
		}
	}

	return true, nil
}

// write new checkpoint before full sync
func PrepareFullSyncCkpt(ckptManager Writer, dynamoSession *dynamodb.DynamoDB,
	dynamoStreams *dynamodbstreams.DynamoDBStreams) (map[string]*dynamodbstreams.Stream, error) {
	// fetch source tables
	sourceTableList, err := utils.FetchTableList(dynamoSession)
	if err != nil {
		return nil, fmt.Errorf("fetch dynamodb table list failed[%v]", err)
	}

	// filter
	sourceTableList = filter.FilterList(sourceTableList)
	sourceTableMap := utils.StringListToMap(sourceTableList)

	LOG.Info("traverse current streams")
	// traverse streams to check whether all streams enabled
	for {
		streamList, err := dynamoStreams.ListStreams(&dynamodbstreams.ListStreamsInput{})
		if err != nil {
			return nil, fmt.Errorf("fetch dynamodb stream list failed[%v]", err)
		}

		for _, stream := range streamList.Streams {
			LOG.Info("check stream with table[%v]", *stream.TableName)
			describeStreamResult, err := dynamoStreams.DescribeStream(&dynamodbstreams.DescribeStreamInput{
				StreamArn: stream.StreamArn,
			})
			if err != nil {
				return nil, fmt.Errorf("describe stream[%v] with table[%v] failed[%v]", *stream.StreamArn,
					*stream.TableName, err)
			}

			if *describeStreamResult.StreamDescription.StreamStatus == "DISABLED" {
				// stream is disabled
				continue
			}

			if filter.IsFilter(*stream.TableName) {
				LOG.Info("table[%v] filtered", *stream.TableName)
				continue
			}

			// remove from sourceTableMap marks this stream already enabled
			delete(sourceTableMap, *stream.TableName)
		}

		if streamList.LastEvaluatedStreamArn == nil {
			// end
			break
		}
	}

	if len(sourceTableMap) != 0 {
		LOG.Info("enable and marks streams: %v", sourceTableMap)
		// enable and marks new stream
		for table := range sourceTableMap {
			_, err := dynamoSession.UpdateTable(&dynamodb.UpdateTableInput{
				TableName: aws.String(table),
				StreamSpecification: &dynamodb.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: aws.String(StreamViewType),
				},
			})
			if err != nil {
				return nil, fmt.Errorf("enable stream for table[%v] failed[%v]", table, err)
			}
		}

		LOG.Info("wait new streams created[%v]...", sourceTableMap)
		time.Sleep(30 * time.Second) // wait new stream created
	}

	// re-create a new map to mark whether current table exists on the target checkpoint table
	sourceCkptTableMap := utils.StringListToMap(sourceTableList)

	LOG.Info("traverse all streams: %v", sourceCkptTableMap)
	streamMap := make(map[string]*dynamodbstreams.Stream)
	// traverse streams
	for {
		streamList, err := dynamoStreams.ListStreams(&dynamodbstreams.ListStreamsInput{})
		if err != nil {
			return nil, fmt.Errorf("fetch dynamodb stream list failed[%v]", err)
		}

		for _, stream := range streamList.Streams {
			if filter.IsFilter(*stream.TableName) {
				LOG.Info("table[%v] filtered", *stream.TableName)
				continue
			}
			LOG.Info("check checkpoint stream with table[%v]", *stream.TableName)

			describeStreamResult, err := dynamoStreams.DescribeStream(&dynamodbstreams.DescribeStreamInput{
				StreamArn: stream.StreamArn,
			})
			if err != nil {
				return nil, fmt.Errorf("describe stream[%v] with table[%v] failed[%v]", stream.StreamArn,
					stream.TableName, err)
			}

			if *describeStreamResult.StreamDescription.StreamStatus == "DISABLED" {
				// stream is disabled
				LOG.Info("stream with table[%v] disabled", *stream.TableName)
				continue
			}

			if _, ok := sourceCkptTableMap[*stream.TableName]; !ok {
				LOG.Info("table[%v] already in checkpoint table", *stream.TableName)
				continue
			}
			delete(sourceCkptTableMap, *stream.TableName)

			// add into steamMap which will be used in incr-sync fetcher
			streamMap[*stream.TableName] = stream

			// traverse shard
			LOG.Info("table[%v] doesn't have checkpoint, try to insert", *stream.TableName)
			rootNode := utils.BuildShardTree(describeStreamResult.StreamDescription.Shards, *stream.TableName,
				*stream.StreamArn)

			if tree, err := utils.PrintShardTree(rootNode); err != nil {
				LOG.Info("table[%v] traverse to print tree failed[%v]", *stream.TableName, err)
			} else {
				LOG.Info("traverse stream tree for table[%v]: \n-----\n%v\n-----", *stream.TableName, tree)
			}

			err = utils.TraverseShard(rootNode, func(node *utils.ShardNode) error {
				var father string
				if node.Shard.ParentShardId != nil {
					father = *node.Shard.ParentShardId
				}
				ckpt := &Checkpoint{
					ShardId:        *node.Shard.ShardId,
					FatherId:       father,
					SequenceNumber: "",
					Status:         StatusNotProcess,
					WorkerId:       "unknown-worker",
					IteratorType:   IteratorTypeAtSequence,
				}

				if node.Shard.SequenceNumberRange.EndingSequenceNumber != nil {
					// shard already closed
					ckpt.Status = StatusNoNeedProcess
					LOG.Info("insert table[%v] checkpoint[%v]: %v", *stream.TableName, *ckpt, ckpt.Status)
					return ckptManager.Insert(ckpt, *stream.TableName)
				} else {
					ckpt.Status = StatusPrepareProcess
					ckpt.IteratorType = IteratorTypeLatest
					outShardIt, err := dynamoStreams.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
						ShardId:           node.Shard.ShardId,
						ShardIteratorType: aws.String(IteratorTypeLatest),
						StreamArn:         describeStreamResult.StreamDescription.StreamArn,
					})
					if err != nil {
						return fmt.Errorf("construct shard[%v] iterator failed[%v]", node.Shard.ShardId, err)
					}
					// ckpt.ShardIt = *outShardIt.ShardIterator
					ckpt.ShardIt = InitShardIt

					// fetch sequence number based on first record
					if seq, approximate, err := fetchSeqNumber(outShardIt.ShardIterator, dynamoStreams, node.Table); err != nil {
						return fmt.Errorf("fetch shard[%v] sequence number failed[%v]", node.Shard.ShardId, err)
					} else if seq != "" {
						ckpt.SequenceNumber = seq
						ckpt.ApproximateTime = approximate
					} else {
						// set the shard start sequence number as sequence number if no data return
						ckpt.SequenceNumber = *node.Shard.SequenceNumberRange.StartingSequenceNumber
					}

					// v1.0.4: do not set
					// set shard iterator map which will be used in incr-sync
					// GlobalShardIteratorMap.Set(*node.Shard.ShardId, *outShardIt.ShardIterator)

					LOG.Info("insert table[%v] checkpoint[%v]", *stream.TableName, *ckpt)
					if err = ckptManager.Insert(ckpt, *stream.TableName); err != nil {
						return fmt.Errorf("shard[%v] insert checkpoint failed[%v]", *node.Shard.ShardId, err)
					}

					if len(node.Sons) != 0 {
						LOG.Info("shard[%v] has sons[%v], wait current node finished", *node.Shard.ShardId, node.Sons)
						return utils.StopTraverseSonErr
					}
				}
				return nil
			})
			if err != nil {
				LOG.Crashf("traverse stream tree for table[%v] failed[%v]", *stream.TableName, err)
			}
		}

		if streamList.LastEvaluatedStreamArn == nil {
			// end
			break
		}
	}

	return streamMap, nil
}

// fetch first sequence number based on given shardIt
func fetchSeqNumber(shardIt *string, dynamoStreams *dynamodbstreams.DynamoDBStreams, table string) (string, string, error) {
	LOG.Info("fetch sequence number of shard[%v] table[%v]", *shardIt, table)
	// retry 5 times
	for i := 0; i < 5; i++ {
		records, err := dynamoStreams.GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: shardIt,
			Limit:         aws.Int64(1),
		})
		if err != nil {
			return "", "", err
		}

		if len(records.Records) > 0 {
			return *records.Records[0].Dynamodb.SequenceNumber,
				records.Records[0].Dynamodb.ApproximateCreationDateTime.String(), nil
		}
		time.Sleep(5 * time.Second)

		shardIt = records.NextShardIterator
	}
	LOG.Info("fetch sequence number of shard[%v] table[%v]: not found, return empty", *shardIt, table)

	// what if no data? return empty
	return "", "", nil
}
