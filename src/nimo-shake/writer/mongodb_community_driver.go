package writer

import (
	"nimo-shake/common"
	"fmt"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"nimo-shake/configure"
	"strings"
	"context"
	"github.com/vinllen/mongo-go-driver/mongo"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	"github.com/vinllen/mongo-go-driver/mongo/options"
)

const (
	NumInitialChunks = 1024
)

type MongoCommunityWriter struct {
	Name           string
	ns             utils.NS
	conn           *utils.MongoCommunityConn
	primaryIndexes []*dynamodb.KeySchemaElement
	ctx    context.Context
	upsertOption *options.ReplaceOptions
}

func NewMongoCommunityWriter(name, address string, ns utils.NS) *MongoCommunityWriter {
	targetConn, err := utils.NewMongoCommunityConn(address, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("create mongodb community connection error[%v]", err)
		return nil
	}

	upsertOptions := new(options.ReplaceOptions)
	upsertOptions.SetUpsert(true)
	return &MongoCommunityWriter{
		Name: name,
		ns:   ns,
		conn: targetConn,
		ctx: context.Background(), // default
		upsertOption: upsertOptions,
	}
}

func (mcw *MongoCommunityWriter) String() string {
	return mcw.Name
}

func (mcw *MongoCommunityWriter) GetSession() interface{} {
	return mcw.conn.Client
}

func (mcw *MongoCommunityWriter) PassTableDesc(tableDescribe *dynamodb.TableDescription) {
	mcw.primaryIndexes = tableDescribe.KeySchema
}

func (mcw *MongoCommunityWriter) CreateTable(tableDescribe *dynamodb.TableDescription) error {
	// parse primary key with sort key
	allIndexes := tableDescribe.AttributeDefinitions
	primaryIndexes := tableDescribe.KeySchema
	globalSecondaryIndexes := tableDescribe.GlobalSecondaryIndexes

	mcw.primaryIndexes = primaryIndexes
	LOG.Info("%s table[%s] primary index length: %v", mcw.String(), *tableDescribe.TableName, len(mcw.primaryIndexes))

	// parse index type
	parseMap := utils.ParseIndexType(allIndexes)

	// create primary key if has
	if len(primaryIndexes) == 0 {
		LOG.Info("%s no index found", mcw)
		return nil
	}

	// check if legal
	if len(primaryIndexes) > 2 {
		return fmt.Errorf("%s illegal primary index[%v] number, should <= 2", mcw, len(primaryIndexes))
	}

	if conf.Options.FullEnableIndexPrimary {
		LOG.Info("%s try create primary index", mcw)
		// create primary index
		if err := mcw.createPrimaryIndex(primaryIndexes, parseMap); err != nil {
			return err
		}

		// create user index
		if conf.Options.FullEnableIndexUser {
			LOG.Info("%s try create user index", mcw)
			// create user index
			if err := mcw.createUserIndex(globalSecondaryIndexes, parseMap); err != nil {
				return err
			}
		}
	}

	return nil
}

func (mcw *MongoCommunityWriter) DropTable() error {
	// error will be depressed when ns not found
	err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).Drop(mcw.ctx)
	return err
}

func (mcw *MongoCommunityWriter) WriteBulk(input []interface{}) error {
	if len(input) == 0 {
		return nil
	}

	// convert input array to models list
	models := make([]mongo.WriteModel, len(input))
	for i := range models {
		models[i] = &mongo.InsertOneModel{Document: input[i]}
		LOG.Debug("WriteBulk: %v", input[i])
	}

	_, err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).BulkWrite(nil, models)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key error") {
			LOG.Warn("%s duplicated document found[%v]. reinsert or update", err, mcw)
			if !conf.Options.FullExecutorInsertOnDupUpdate || len(mcw.primaryIndexes) == 0 {
				LOG.Error("full.executor.insert_on_dup_update==[%v], primaryIndexes length[%v]", conf.Options.FullExecutorInsertOnDupUpdate,
					len(mcw.primaryIndexes))
				return err
			}

			// 1. generate index list
			indexList := make([]interface{}, len(input))
			for i, ele := range input {
				inputData := ele.(bson2.M)
				index := make(bson2.M, len(mcw.primaryIndexes))
				for _, primaryIndex := range mcw.primaryIndexes {
					// currently, we only support convert type == 'convert', so there is no type inside
					key := *primaryIndex.AttributeName
					if _, ok := inputData[key]; !ok {
						LOG.Error("primary key[%v] is not exists on input data[%v]",
							*primaryIndex.AttributeName, inputData)
					} else {
						index[key] = inputData[key]
					}
				}
				indexList[i] = index
			}

			LOG.Debug(indexList)

			return mcw.updateOnInsert(input, indexList)
		}
		return fmt.Errorf("%s insert docs with length[%v] into ns[%s] of dest mongo failed[%v]. first doc: %v",
			mcw, len(input), mcw.ns, err, input[0])
	}
	return nil
}

func (mcw *MongoCommunityWriter) Close() {
	mcw.conn.Close()
}

func (mcw *MongoCommunityWriter) Insert(input []interface{}, index []interface{}) error {
	// convert input array to models list
	models := make([]mongo.WriteModel, len(input))
	for i := range models {
		models[i] = &mongo.InsertOneModel{Document: input[i]}
	}

	_, err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).BulkWrite(nil, models)

	if err != nil {
		if utils.MongodbIgnoreError(err, "i", false) {
			LOG.Warn("%s ignore error[%v] when insert", mcw, err)
			return nil
		}

		// duplicate key
		if strings.Contains(err.Error(), "duplicate key error") {
			if conf.Options.IncreaseExecutorInsertOnDupUpdate {
				LOG.Warn("%s duplicated document found[%v]. reinsert or update", mcw, err)
				return mcw.updateOnInsert(input, index)
			}
		}
		return err
	}
	return nil
}

func (mcw *MongoCommunityWriter) updateOnInsert(input []interface{}, index []interface{}) error {
	// upsert one by one
	for i := range input {
		LOG.Debug("upsert: selector[%v] update[%v]", index[i], input[i])
		// _, err := mw.conn.Session.DB(mw.ns.Database).C(mw.ns.Collection).Upsert(index[i], input[i])
		_, err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).ReplaceOne(nil, index[i], input[i], mcw.upsertOption)
		if err != nil {
			if utils.MongodbIgnoreError(err, "u", true) {
				LOG.Warn("%s ignore error[%v] when upsert", mcw, err)
				return nil
			}

			if strings.Contains(err.Error(), "duplicate key error") {
				// ignore duplicate key
				continue
			}

			return err
		}
	}
	return nil
}

func (mcw *MongoCommunityWriter) Delete(index []interface{}) error {
	// convert input array to models list
	models := make([]mongo.WriteModel, len(index))
	for i := range models {
		models[i] = &mongo.DeleteOneModel{Filter: index[i]}
	}

	_, err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).BulkWrite(nil, models)
	if err != nil {
		LOG.Warn(err)
		// always ignore ns not found error
		if utils.MongodbIgnoreError(err, "d", true) {
			LOG.Warn("%s ignore error[%v] when delete", mcw, err)
			return nil
		}

		return err
	}

	return nil
}

func (mcw *MongoCommunityWriter) Update(input []interface{}, index []interface{}) error {
	models := make([]mongo.WriteModel, len(index))
	for i := range models {
		uom := &mongo.ReplaceOneModel{Filter: index[i], Replacement: input[i]}
		if conf.Options.IncreaseExecutorUpsert {
			uom.SetUpsert(true)
		}
		models[i] = uom
	}

	_, err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).BulkWrite(nil, models)
	if err != nil {
		LOG.Warn(err)

		if strings.Contains(err.Error(), "duplicate key error") {
			return mcw.updateOnInsert(input, index)
		}

		return err
	}

	return nil
}

func (mcw *MongoCommunityWriter) createPrimaryIndex(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string) error {
	primaryKeyWithType, err := mcw.createSingleIndex(primaryIndexes, parseMap, true)
	if err != nil {
		return err
	}

	// write shard key if target mongodb is sharding
	if conf.Options.TargetMongoDBType == utils.TargetMongoDBTypeSharding {
		res := mcw.conn.Client.Database("admin").RunCommand(nil, bson2.D{
			{"enablesharding", mcw.ns.Database},
		})
		if err := res.Err(); err != nil {
			if strings.Contains(err.Error(), "sharding already enabled") == false {
				return fmt.Errorf("enable sharding failed[%v]", err)
			}
			LOG.Warn("ns[%s] sharding already enabled: %v", mcw.ns, err)
		}

		res = mcw.conn.Client.Database("admin").RunCommand(nil, bson2.D{
			{"shardCollection", mcw.ns.Str()},
			{"key", bson2.M{primaryKeyWithType: "hashed"}},
			{"options", bson2.M{"numInitialChunks": NumInitialChunks}},
		})
		if err := res.Err(); err != nil {
			return fmt.Errorf("shard collection[%s] failed[%v]", mcw.ns, err)
		}
	}

	return nil
}

func (mcw *MongoCommunityWriter) createUserIndex(globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndexDescription, parseMap map[string]string) error {
	for _, gsi := range globalSecondaryIndexes {
		primaryIndexes := gsi.KeySchema
		// duplicate index will be ignored by MongoDB
		if _, err := mcw.createSingleIndex(primaryIndexes, parseMap, false); err != nil {
			LOG.Error("ns[%s] create users' single index failed[%v]", mcw.ns, err)
			return err
		}
	}
	return nil
}

func (mcw *MongoCommunityWriter) createSingleIndex(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string,
	isPrimaryKey bool) (string, error) {
	primaryKey, sortKey, err := utils.ParsePrimaryAndSortKey(primaryIndexes, parseMap)
	if err != nil {
		return "", fmt.Errorf("parse primary and sort key failed[%v]", err)
	}

	primaryKeyWithType := mcw.fetchKey(primaryKey, parseMap[primaryKey])
	indexList := make([]string, 0, 2)
	indexList = append(indexList, primaryKeyWithType)
	if sortKey != "" {
		indexList = append(indexList, mcw.fetchKey(sortKey, parseMap[sortKey]))
	}

	LOG.Info("ns[%s] single index[%v] list[%v]", mcw.ns, primaryKeyWithType, indexList)

	// primary key should be unique
	unique := isPrimaryKey

	ctx := context.Background()

	// create union unique index
	if len(indexList) >= 2 {
		LOG.Info("create union-index isPrimary[%v]: %v", isPrimaryKey, indexList)

		var keysMap bson2.D
		for _, ele := range indexList {
			keysMap = append(keysMap, bson2.E{ele, 1})
		}
		indexModel := mongo.IndexModel{
			Keys: keysMap,
			Options: &options.IndexOptions{},
		}
		indexModel.Options.SetUnique(unique)
		indexModel.Options.SetBackground(true)
		_, err := mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).Indexes().CreateOne(ctx, indexModel)
		if err != nil {
			return "", fmt.Errorf("create primary union unique[%v] index failed[%v]", unique, err)
		}
	}

	var indexType interface{}
	indexType = "hashed"
	if conf.Options.TargetMongoDBType == utils.TargetMongoDBTypeReplica {
		indexType = 1
	}
	if len(indexList) >= 2 {
		// unique has already be set on the above index
		unique = false
	} else if unique {
		// must be range if only has 1 key
		indexType = 1
	}

	indexModel := mongo.IndexModel{
		Keys: bson2.M{
			primaryKeyWithType: indexType,
		},
		Options: &options.IndexOptions{},
	}
	indexModel.Options.SetUnique(unique)
	indexModel.Options.SetBackground(true)

	_, err = mcw.conn.Client.Database(mcw.ns.Database).Collection(mcw.ns.Collection).Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return "", fmt.Errorf("create primary[%v] %v index failed[%v]", isPrimaryKey, indexType, err)
	}

	return primaryKeyWithType, nil
}

func (mcw *MongoCommunityWriter) fetchKey(key, tp string) string {
	switch conf.Options.ConvertType {
	case utils.ConvertTypeChange:
		fallthrough
	case utils.ConvertTypeSame:
		return key
	case utils.ConvertTypeRaw:
		return fmt.Sprintf("%s.%s", key, tp)
	}
	return ""
}
