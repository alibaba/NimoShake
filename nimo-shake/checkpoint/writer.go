package checkpoint

import (
	"reflect"

	LOG "github.com/vinllen/log4go"
)

type Writer interface {
	// find current status
	FindStatus() (string, error)

	// update status
	UpdateStatus(status string) error

	// extract all checkpoint
	ExtractCheckpoint() (map[string]map[string]*Checkpoint, error)

	// extract single checkpoint
	ExtractSingleCheckpoint(table string) (map[string]*Checkpoint, error)

	// insert checkpoint
	Insert(ckpt *Checkpoint, table string) error

	// update checkpoint
	Update(shardId string, ckpt *Checkpoint, table string) error

	// update with set
	UpdateWithSet(shardId string, input map[string]interface{}, table string) error

	// query
	Query(shardId string, table string) (*Checkpoint, error)

	// insert incrSyncCacheFile
	IncrCacheFileInsert(table string, shardId string, fileName string, lastSequenceNumber string, time string) error

	// drop
	DropAll() error
}

func NewWriter(name, address, db string) Writer {
	var w Writer = nil
	switch name {
	case CheckpointWriterTypeMongo:
		w = NewMongoWriter(address, db)
	case CheckpointWriterTypeFile:
		w = NewFileWriter(db)
	default:
		LOG.Crashf("unknown checkpoint writer[%v]", name)
	}

	if IsNil(w) {
		LOG.Crashf("create checkpoint writer[%v] failed", name)
	}

	return w
}

func IsNil(w Writer) bool {
	if w == nil {
		return true
	}

	v := reflect.ValueOf(w)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
