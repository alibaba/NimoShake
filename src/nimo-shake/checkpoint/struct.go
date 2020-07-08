package checkpoint

import (
	"nimo-shake/filter"
	"sync"
)

const (
	CheckpointWriterTypeMongo     = "mongo"
	CheckpointWriterTypeFile      = "file"
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
	ShardId         string `bson:"ShardId" json:"ShardId"`           // shard id
	FatherId        string `bson:"FatherId" json:"FatherId"`         // father id
	SequenceNumber  string `bson:"SeqNum" json:"SeqNum"`             // checkpoint
	Status          string `bson:"Status" json:"Status"`               // status
	WorkerId        string `bson:"WorkerId" json:"WorkerId"`         // thread number
	IteratorType    string `bson:"IteratorType" json:"IteratorType"` // "LATEST" or "AT_SEQUENCE_NUMBER"
	ShardIt         string `bson:"ShardIt" json:"ShardIt"`           // only used when IteratorType == "LATEST"
	UpdateTimestamp string `bson:"UpdateDate" json:"UpdateDate"`
}

type Status struct {
	Key   string `bson:"Key" json:"Key"`                   // key -> CheckpointStatusKey
	Value string `bson:"StatusValue" json:"StatusValue"` // CheckpointStatusValueFullSync or CheckpointStatusValueIncrSync
}

/*---------------------------------------*/

var (
	GlobalShardIteratorMap = ShardIteratorMap{
		mp: make(map[string]string),
	}
)

type ShardIteratorMap struct {
	mp   map[string]string
	lock sync.Mutex
}

func (sim *ShardIteratorMap) Set(key, iterator string) bool {
	sim.lock.Lock()
	defer sim.lock.Unlock()

	if _, ok := sim.mp[key]; ok {
		return false
	}

	sim.mp[key] = iterator
	return false
}

func (sim *ShardIteratorMap) Get(key string) (string, bool) {
	sim.lock.Lock()
	defer sim.lock.Unlock()

	it, ok := sim.mp[key]
	return it, ok
}

func (sim *ShardIteratorMap) Delete(key string) bool {
	sim.lock.Lock()
	defer sim.lock.Unlock()

	if _, ok := sim.mp[key]; ok {
		delete(sim.mp, key)
		return true
	}
	return false
}

/*---------------------------------------*/

func FilterCkptCollection(collection string) bool {
	return collection == CheckpointStatusTable || filter.IsFilter(collection)
}

func IsStatusProcessing(status string) bool {
	return status == StatusPrepareProcess || status == StatusInProcessing || status == StatusWaitFather
}

func IsStatusNoNeedProcess(status string) bool {
	return status == StatusDone || status == StatusNoNeedProcess
}
