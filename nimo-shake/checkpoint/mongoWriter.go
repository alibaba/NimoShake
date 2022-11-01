package checkpoint

import (
	"fmt"

	"nimo-shake/common"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

type MongoWriter struct {
	address string
	conn    *utils.MongoConn
	db      string
}

func NewMongoWriter(address, db string) *MongoWriter {
	targetConn, err := utils.NewMongoConn(address, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("create mongodb with address[%v] db[%v] connection error[%v]", address, db, err)
		return nil
	}

	return &MongoWriter{
		address: address,
		conn:    targetConn,
		db:      db,
	}
}

func (mw *MongoWriter) FindStatus() (string, error) {
	var query Status
	if err := mw.conn.Session.DB(mw.db).C(CheckpointStatusTable).Find(bson.M{"Key": CheckpointStatusKey}).
		One(&query); err != nil {
		if err.Error() == utils.NotFountErr {
			return CheckpointStatusValueEmpty, nil
		}
		return "", err
	} else {
		return query.Value, nil
	}
}

func (mw *MongoWriter) UpdateStatus(status string) error {
	update := Status{
		Key:   CheckpointStatusKey,
		Value: status,
	}
	_, err := mw.conn.Session.DB(mw.db).C(CheckpointStatusTable).Upsert(bson.M{"Key": CheckpointStatusKey}, update)
	return err
}

func (mw *MongoWriter) ExtractCheckpoint() (map[string]map[string]*Checkpoint, error) {
	// extract checkpoint from mongodb, every collection checkpoint have independent collection(table)
	ckptMap := make(map[string]map[string]*Checkpoint)

	collectionList, err := mw.conn.Session.DB(mw.db).CollectionNames()
	if err != nil {
		return nil, fmt.Errorf("fetch checkpoint collection list failed[%v]", err)
	}
	for _, table := range collectionList {
		if FilterCkptCollection(table) {
			continue
		}

		innerMap, err := mw.ExtractSingleCheckpoint(table)
		if err != nil {
			return nil, err
		}
		ckptMap[table] = innerMap
	}

	return ckptMap, nil
}

func (mw *MongoWriter) ExtractSingleCheckpoint(table string) (map[string]*Checkpoint, error) {
	innerMap := make(map[string]*Checkpoint)
	data := make([]*Checkpoint, 0)
	err := mw.conn.Session.DB(mw.db).C(table).Find(bson.M{}).All(&data)
	if err != nil {
		return nil, err
	}

	for _, ele := range data {
		innerMap[ele.ShardId] = ele
	}

	return innerMap, nil
}

func (mw *MongoWriter) Insert(ckpt *Checkpoint, table string) error {
	return mw.conn.Session.DB(mw.db).C(table).Insert(*ckpt)
}

func (mw *MongoWriter) Update(shardId string, ckpt *Checkpoint, table string) error {
	return mw.conn.Session.DB(mw.db).C(table).Update(bson.M{"ShardId": shardId}, *ckpt)
}

func (mw *MongoWriter) UpdateWithSet(shardId string, input map[string]interface{}, table string) error {
	return mw.conn.Session.DB(mw.db).C(table).Update(bson.M{"ShardId": shardId}, bson.M{"$set": input})
}

func (mw *MongoWriter) Query(shardId string, table string) (*Checkpoint, error) {
	var res Checkpoint
	err := mw.conn.Session.DB(mw.db).C(table).Find(bson.M{"ShardId": shardId}).One(&res)
	return &res, err
}

func (mw *MongoWriter) DropAll() error {
	return mw.conn.Session.DB(mw.db).DropDatabase()
}

func (fw *MongoWriter) IncrCacheFileInsert(table string, shardId string, fileName string,
	lastSequenceNumber string, time string) error {

	// write cachefile struct to db
	return nil
}
