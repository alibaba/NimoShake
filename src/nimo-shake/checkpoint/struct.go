package checkpoint

import (
	"nimo-shake/common"
	"github.com/vinllen/mgo/bson"
	"nimo-shake/filter"
)

const (
	CheckpointStatusTable         = "status_table"
	CheckpointStatusKey           = "status_key"
	CheckpointStatusValueEmpty    = ""
	CheckpointStatusValueFullSync = "full_sync"
	CheckpointStatusValueIncrSync = "incr_sync"

	// 0: not process; 1: no need to process; 2: prepare stage 3: in processing; 4: wait father finish, 5: done
	StatusNotProcess     = "not process"
	StatusNoNeedProcess  = "no need to process"
	StatusPrepareProcess = "prepare stage"
	StatusInProcessing   = "in processing"
	StatusWaitFather     = "wait father finish"
	StatusDone           = "done"

	IteratorTypeLatest   = "LATEST"
	IteratorTypeSequence = "AT_SEQUENCE_NUMBER"

	StreamViewType = "NEW_AND_OLD_IMAGES"

	FieldShardId      = "shard_id"
	FieldShardIt      = "shard_it"
	FieldStatus       = "status"
	FieldSeqNum       = "seq_num"
	FieldIteratorType = "iterator_type"
	FieldTimestamp    = "update_date"
)

type Checkpoint struct {
	ShardId         string `bson:"shard_id"`      // shard id
	FatherId        string `bson:"father_id"`     // father id
	SequenceNumber  string `bson:"seq_num"`       // checkpoint
	Status          string `bson:"status"`        // status
	WorkerId        string `bson:"worker_id"`     // thread number
	IteratorType    string `bson:"iterator_type"` // "LATEST" or "AT_SEQUENCE_NUMBER"
	ShardIt         string `bson:"shard_it"`      // only used when IteratorType == "LATEST"
	UpdateTimestamp string `bson:"update_date"`
}

type Status struct {
	Key   string `bson:"key"`          // key -> CheckpointStatusKey
	Value string `bson:"status_value"` // CheckpointStatusValueFullSync or CheckpointStatusValueIncrSync
}

func FindStatus(targetClient *utils.MongoConn, db, collection string) (string, error) {
	var query Status
	if err := targetClient.Session.DB(db).C(collection).Find(bson.M{"key": CheckpointStatusKey}).One(&query); err != nil {
		if err.Error() == utils.NotFountErr {
			return CheckpointStatusValueEmpty, nil
		}
		return "", err
	} else {
		return query.Value, nil
	}
}

func UpsertStatus(targetClient *utils.MongoConn, db, collection string, input string) error {
	update := Status{
		Key:   CheckpointStatusKey,
		Value: input,
	}
	_, err := targetClient.Session.DB(db).C(collection).Upsert(bson.M{"key": CheckpointStatusKey}, update)
	return err
}

func FilterCkptCollection(collection string) bool {
	return collection == CheckpointStatusTable || filter.IsFilter(collection)
}

func IsStatusProcessing(status string) bool {
	return status == StatusPrepareProcess || status == StatusInProcessing || status == StatusWaitFather
}

func IsStatusNoNeedProcess(status string) bool {
	return status == StatusDone || status == StatusNoNeedProcess
}
