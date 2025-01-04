package checkpoint

import (
	"context"
	"fmt"

	utils "nimo-shake/common"

	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoWriter struct {
	address string
	//conn    *utils.MongoConn
	nconn *utils.MongoCommunityConn
	db    string
}

func NewMongoWriter(address, db string) *MongoWriter {
	targetConn, err := utils.NewMongoCommunityConn(address, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("create mongodb with address[%v] db[%v] connection error[%v]", address, db, err)
		return nil
	}

	return &MongoWriter{
		address: address,
		nconn:   targetConn,
		db:      db,
	}
}

func (mw *MongoWriter) FindStatus() (string, error) {
	var query Status
	if err := mw.nconn.Client.Database(mw.db).Collection(CheckpointStatusTable).FindOne(context.TODO(),
		bson.M{"Key": CheckpointStatusKey}).Decode(&query); err != nil {
		if err == mongo.ErrNoDocuments {
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

	opts := options.Update().SetUpsert(true)
	filter := bson.M{"Key": CheckpointStatusKey}
	updateStr := bson.M{"$set": update}
	_, err := mw.nconn.Client.Database(mw.db).Collection(CheckpointStatusTable).UpdateOne(context.TODO(), filter, updateStr, opts)
	return err
}

func (mw *MongoWriter) ExtractCheckpoint() (map[string]map[string]*Checkpoint, error) {
	// extract checkpoint from mongodb, every collection checkpoint have independent collection(table)
	ckptMap := make(map[string]map[string]*Checkpoint)

	collectionList, err := mw.nconn.Client.Database(mw.db).ListCollectionNames(context.TODO(), bson.M{})
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

	cursor, err := mw.nconn.Client.Database(mw.db).Collection(table).Find(context.TODO(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var elem Checkpoint
		if err := cursor.Decode(&elem); err != nil {
			return nil, err
		}
		data = append(data, &elem)
	}

	for _, ele := range data {
		innerMap[ele.ShardId] = ele
	}

	return innerMap, nil
}

func (mw *MongoWriter) Insert(ckpt *Checkpoint, table string) error {
	_, err := mw.nconn.Client.Database(mw.db).Collection(table).InsertOne(context.TODO(), ckpt)

	return err
}

func (mw *MongoWriter) Update(shardId string, ckpt *Checkpoint, table string) error {

	filter := bson.M{"ShardId": shardId}
	updateStr := bson.M{"$set": ckpt}
	_, err := mw.nconn.Client.Database(mw.db).Collection(table).UpdateOne(context.TODO(), filter, updateStr)
	return err
}

func (mw *MongoWriter) UpdateWithSet(shardId string, input map[string]interface{}, table string) error {

	filter := bson.M{"ShardId": shardId}
	updateStr := bson.M{"$set": input}
	_, err := mw.nconn.Client.Database(mw.db).Collection(table).UpdateOne(context.TODO(), filter, updateStr)
	return err
}

func (mw *MongoWriter) Query(shardId string, table string) (*Checkpoint, error) {
	var res Checkpoint
	err := mw.nconn.Client.Database(mw.db).Collection(table).FindOne(context.TODO(), bson.M{"ShardId": shardId}).Decode(&res)

	return &res, err
}

func (mw *MongoWriter) DropAll() error {
	return mw.nconn.Client.Database(mw.db).Drop(context.TODO())
}
