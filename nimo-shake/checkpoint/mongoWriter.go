package checkpoint

import (
	"context"
	"fmt"
	"github.com/vinllen/mongo-go-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"

	"nimo-shake/common"

	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
)

type MongoWriter struct {
	address string
	conn    *utils.MongoCommunityConn
	db      string
}

func NewMongoWriter(address, db string) *MongoWriter {
	targetConn, err := utils.NewMongoCommunityConn(address, utils.ConnectModePrimary, true)
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

	ret := mw.conn.Client.Database(mw.db).Collection(CheckpointStatusTable).FindOne(
		context.Background(),
		bson.M{
			"Key": CheckpointStatusKey,
		},
	)

	if err := ret.Decode(&query); err != nil {
		if err.Error() == mongo.ErrNoDocuments.Error() {
			//if err.Error() == utils.NotFountErr {
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
	_, err := mw.conn.Client.Database(mw.db).Collection(CheckpointStatusTable).UpdateOne(context.Background(),
		bson.M{"Key": CheckpointStatusKey}, bson.M{"$set": update}, options.Update().SetUpsert(true))
	return err
}

func (mw *MongoWriter) ExtractCheckpoint() (map[string]map[string]*Checkpoint, error) {
	// extract checkpoint from mongodb, every collection checkpoint have independent collection(table)
	ckptMap := make(map[string]map[string]*Checkpoint)

	collectionList, err := mw.conn.Client.Database(mw.db).ListCollectionNames(context.Background(), bson.M{})
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
	cursor, err := mw.conn.Client.Database(mw.db).Collection(table).Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}

	for cursor.Next(nil) {
		var ele Checkpoint
		err = cursor.Decode(&ele)
		if err != nil {
			return nil, err
		}

		innerMap[ele.ShardId] = &ele
	}

	return innerMap, nil
}

func (mw *MongoWriter) Insert(ckpt *Checkpoint, table string) error {
	_, err := mw.conn.Client.Database(mw.db).Collection(table).InsertOne(context.Background(), *ckpt)
	return err
}

func (mw *MongoWriter) Update(shardId string, ckpt *Checkpoint, table string) error {
	_, err := mw.conn.Client.Database(mw.db).Collection(table).ReplaceOne(context.Background(),
		bson.M{"ShardId": shardId}, *ckpt)
	return err
}

func (mw *MongoWriter) UpdateWithSet(shardId string, input map[string]interface{}, table string) error {
	_, err := mw.conn.Client.Database(mw.db).Collection(table).UpdateOne(context.Background(),
		bson.M{"ShardId": shardId}, bson.M{"$set": input})
	return err
}

func (mw *MongoWriter) Query(shardId string, table string) (*Checkpoint, error) {
	var res Checkpoint
	err := mw.conn.Client.Database(mw.db).Collection(table).FindOne(context.Background(),
		bson.M{"ShardId": shardId}).Decode(&res)
	return &res, err
}

func (mw *MongoWriter) DropAll() error {
	return mw.conn.Client.Database(mw.db).Drop(context.Background())
}

func (fw *MongoWriter) IncrCacheFileInsert(table string, shardId string, fileName string,
	lastSequenceNumber string, time string) error {

	// write cachefile struct to db
	return nil
}
