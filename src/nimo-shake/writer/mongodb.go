package writer

import (
	"nimo-shake/common"
	"fmt"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"nimo-shake/protocal"
	"nimo-shake/configure"
	"strings"
)

const (
	NumInitialChunks = 1024
)

type MongoWriter struct {
	Name string
	ns   utils.NS
	conn *utils.MongoConn
}

func NewMongoWriter(name, address string, ns utils.NS) *MongoWriter {
	targetConn, err := utils.NewMongoConn(address, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("create mongodb connection error[%v]", err)
		return nil
	}

	return &MongoWriter{
		Name: name,
		ns:   ns,
		conn: targetConn,
	}
}

func (mw *MongoWriter) String() string {
	return mw.Name
}

func (mw *MongoWriter) CreateTable(ns utils.NS, tableDescribe *dynamodb.TableDescription) error {
	// do nothing for mongo
	return nil
}

func (mw *MongoWriter) WriteBulk(input []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	docList := make([]interface{}, 0, len(input))
	for _, ele := range input {
		docList = append(docList, ele.(protocal.RawData).Data)
	}

	if err := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).Insert(docList...); err != nil {
		return fmt.Errorf("%s insert docs with length[%v] into ns[%s] of dest mongo failed[%v]. first doc: %v",
			mw, len(docList), mw.ns, err, docList[0])
	}
	return nil
}

func (mw *MongoWriter) CreateIndex(tableDescribe *dynamodb.TableDescription) error {
	// parse primary key with sort key
	allIndexes := tableDescribe.AttributeDefinitions
	primaryIndexes := tableDescribe.KeySchema
	globalSecondaryIndexes := tableDescribe.GlobalSecondaryIndexes

	// parse index type
	parseMap := utils.ParseIndexType(allIndexes)

	// create primary key if has
	if len(primaryIndexes) == 0 {
		LOG.Info("%s no index found", mw)
		return nil
	}

	// check if legal
	if len(primaryIndexes) > 2 {
		return fmt.Errorf("%s illegal primary index[%v] number, should <= 2", mw, len(primaryIndexes))
	}

	if conf.Options.FullEnableIndexPrimary {
		LOG.Info("%s try create primary index", mw)
		// create primary index
		if err := mw.createPrimaryIndex(primaryIndexes, parseMap); err != nil {
			return err
		}

		// create user index
		if conf.Options.FullEnableIndexUser {
			LOG.Info("%s try create user index", mw)
			// create user index
			if err := mw.createUserIndex(globalSecondaryIndexes, parseMap); err != nil {
				return err
			}
		}
	}

	return nil
}

func (mw *MongoWriter) Close() {
	mw.conn.Close()
}

func (mw *MongoWriter) Insert(input []interface{}, index []interface{}) error {
	bulk := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).Bulk()
	bulk.Unordered()
	bulk.Insert(input...)

	if _, err := bulk.Run(); err != nil {
		if utils.MongodbIgnoreError(err, "i", false) {
			LOG.Warn("%s ignore error[%v] when insert", mw, err)
			return nil
		}

		// duplicate key
		if mgo.IsDup(err) {
			if conf.Options.IncreaseExecutorInsertOnDupUpdate {
				LOG.Warn("%s duplicated document found. reinsert or update", mw)
				return mw.updateOnInsert(input, index)
			}
		}
		return err
	}
	return nil
}

func (mw *MongoWriter) updateOnInsert(input []interface{}, index []interface{}) error {
	// upsert one by one
	for i := range input {
		_, err := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).Upsert(index[i], input[i])
		if utils.MongodbIgnoreError(err, "u", false) {
			LOG.Warn("%s ignore error[%v] when upsert", mw, err)
			return nil
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (mw *MongoWriter) Delete(index []interface{}) error {
	bulk := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).Bulk()
	bulk.Unordered()
	bulk.Remove(index...)

	if _, err := bulk.Run(); err != nil {
		if utils.MongodbIgnoreError(err, "i", false) {
			LOG.Warn("%s ignore error[%v] when insert", mw, err)
			return nil
		}

		return err
	}

	return nil
}

func (mw *MongoWriter) Update(input []interface{}, index []interface{}) error {
	updates := make([]interface{}, 0, len(input) * 2)
	for i := range input {
		updates = append(updates, index[i])
		updates = append(updates, input[i])
	}

	bulk := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).Bulk()
	bulk.Update(updates...)

	if _, err := bulk.Run(); err != nil {
		// parse error
		idx, _, _ := utils.FindFirstErrorIndexAndMessage(err.Error())
		if idx == -1 {
			return err
		}

		if utils.MongodbIgnoreError(err, "u", false) {
			return mw.updateOnInsert(input[idx: ], index[idx: ])
		}

		if mgo.IsDup(err) {
			return mw.updateOnInsert(input[idx + 1: ], index[idx + 1: ])
		}
		return err
	}

	return nil
}

func (mw *MongoWriter) createPrimaryIndex(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string) error {
	primaryKeyWithType, err := mw.createSingleIndex(primaryIndexes, parseMap, true)
	if err != nil {
		return err
	}

	// write shard key if target mongodb is sharding
	if conf.Options.TargetMongoDBType == utils.TargetMongoDBTypeSharding {
		err := mw.conn.Session.DB("admin").Run(bson.D{
			{Name: "enablesharding", Value: mw.ns.Database},
		}, nil)
		if err != nil {
			if strings.Contains(err.Error(), "sharding already enabled") == false {
				return fmt.Errorf("enable sharding failed[%v]", err)
			}
			LOG.Warn("ns[%s] sharding already enabled: %v", mw.ns, err)
		}

		err = mw.conn.Session.DB("admin").Run(bson.D{
			{Name: "shardCollection", Value: mw.ns.Str()},
			{Name: "key", Value: bson.M{primaryKeyWithType: "hashed"}},
			{Name: "options", Value: bson.M{"numInitialChunks": NumInitialChunks}},
		}, nil)
		if err != nil {
			return fmt.Errorf("shard collection[%s] failed[%v]", mw.ns, err)
		}
	}

	return nil
}

func (mw *MongoWriter) createUserIndex(globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndexDescription, parseMap map[string]string) error {
	for _, gsi := range globalSecondaryIndexes {
		primaryIndexes := gsi.KeySchema
		if _, err := mw.createSingleIndex(primaryIndexes, parseMap, false); err != nil {
			LOG.Error("ns[%s] create users' single index failed[%v]", mw.ns, err)
			return err
		}
	}
	return nil
}

func (mw *MongoWriter) createSingleIndex(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string,
		isPrimaryKey bool) (string, error) {
	primaryKey, sortKey, err := utils.ParsePrimaryAndSortKey(primaryIndexes, parseMap)
	if err != nil {
		return "", fmt.Errorf("parse primary and sort key failed[%v]", err)
	}

	primaryKeyWithType := fmt.Sprintf("%s.%s", primaryKey, parseMap[primaryKey])
	indexList := make([]string, 0)
	indexList = append(indexList, primaryKeyWithType)
	if sortKey != "" {
		indexList = append(indexList, fmt.Sprintf("%s.%s", sortKey, parseMap[sortKey]))
	}

	LOG.Info("ns[%s] single index[%v] list[%v]", mw.ns, primaryKeyWithType, indexList)

	// create union unique index if input is partition key
	if len(indexList) >= 1 && isPrimaryKey {
		// write index
		index := mgo.Index{
			Key:        indexList,
			Unique:     true,
			Background: true,
		}
		if err := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).EnsureIndex(index); err != nil {
			return "", fmt.Errorf("create primary union unique index failed[%v]", err)
		}
	}

	// create hash key only
	if err := mw.conn.Session.DB(mw.ns.Database).Run(bson.D{
		{Name: "createIndexes", Value: mw.ns.Collection},
		{Name: "indexes", Value: []bson.M{
			{
				"key": bson.M {
					primaryKeyWithType: "hashed",
				},
				"name": fmt.Sprintf("%s_%s", primaryKeyWithType, "hashed"),
			},
		}},
		{Name: "background", Value: true},
	}, nil); err != nil {
		return "", fmt.Errorf("create primary hash index failed[%v]", err)
	}

	return primaryKeyWithType, nil
}
