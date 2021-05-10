package full_sync

import (
	"fmt"
	"strings"

	utils "github.com/alibaba/NimoShake/src/nimo-shake/common"
	conf "github.com/alibaba/NimoShake/src/nimo-shake/configure"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	NumInitialChunks = 1024
)

type Index struct {
	sourceConn *dynamodb.DynamoDB
	targetConn *utils.MongoConn
	ns         utils.NS
}

// deprecated
func NewIndex(sourceConn *dynamodb.DynamoDB, targetConn *utils.MongoConn, ns utils.NS) *Index {
	return &Index{
		sourceConn: sourceConn,
		targetConn: targetConn,
		ns:         ns,
	}
}

func (ix *Index) CreateIndex() error {
	// describe dynamodb table
	out, err := ix.sourceConn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(ix.ns.Collection),
	})
	if err != nil {
		LOG.Info("ns[%s] describe table failed[%v]", ix.ns, err)
		return err
	}

	// parse primary key with sort key
	allIndexes := out.Table.AttributeDefinitions
	primaryIndexes := out.Table.KeySchema
	globalSecondaryIndexes := out.Table.GlobalSecondaryIndexes

	// parse index type
	parseMap := utils.ParseIndexType(allIndexes)

	// create primary key if has
	if len(primaryIndexes) > 0 {
		// check if legal
		if len(primaryIndexes) > 2 {
			err := fmt.Errorf("illegal primary index[%v] number, should <= 2", len(primaryIndexes))
			return err
		}

		if conf.Options.FullEnableIndexPrimary {
			LOG.Info("ns[%s] try create primary index", ix.ns)
			// create primary index
			if err := ix.createPrimaryIndex(primaryIndexes, parseMap); err != nil {
				return err
			}
		}

		if conf.Options.FullEnableIndexUser {
			LOG.Info("ns[%s] try create user index", ix.ns)
			// create user index
			if err := ix.createUserIndex(globalSecondaryIndexes, parseMap); err != nil {
				return err
			}
		}

		LOG.Info("ns[%s] finish creating all indexes", ix.ns)
	}

	return nil
}

func (ix *Index) createPrimaryIndex(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string) error {
	primaryKeyWithType, err := ix.createSingleIndex(primaryIndexes, parseMap, true)
	if err != nil {
		return err
	}

	// write shard key if target mongodb is sharding
	if conf.Options.TargetMongoDBType == utils.TargetMongoDBTypeSharding {
		err := ix.targetConn.Session.DB("admin").Run(bson.D{
			{Name: "enablesharding", Value: ix.ns.Database},
		}, nil)
		if err != nil {
			if strings.Contains(err.Error(), "sharding already enabled") == false {
				return fmt.Errorf("enable sharding failed[%v]", err)
			}
			LOG.Warn("ns[%s] sharding already enabled: %v", ix.ns, err)
		}

		err = ix.targetConn.Session.DB("admin").Run(bson.D{
			{Name: "shardCollection", Value: ix.ns.Str()},
			{Name: "key", Value: bson.M{primaryKeyWithType: "hashed"}},
			{Name: "options", Value: bson.M{"numInitialChunks": NumInitialChunks}},
		}, nil)
		if err != nil {
			return fmt.Errorf("shard collection[%s] failed[%v]", ix.ns, err)
		}
	}

	return nil
}

func (ix *Index) createUserIndex(globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndexDescription, parseMap map[string]string) error {
	for _, gsi := range globalSecondaryIndexes {
		primaryIndexes := gsi.KeySchema
		if _, err := ix.createSingleIndex(primaryIndexes, parseMap, false); err != nil {
			LOG.Error("ns[%s] create users' single index failed[%v]", ix.ns, err)
			return err
		}
	}
	return nil
}

func (ix *Index) createSingleIndex(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string,
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

	LOG.Info("ns[%s] single index[%v] list[%v]", ix.ns, primaryKeyWithType, indexList)

	// create union unique index if input is partition key
	if len(indexList) >= 1 && isPrimaryKey {
		// write index
		index := mgo.Index{
			Key:    indexList,
			Unique: true,
		}
		if err := ix.targetConn.Session.DB(ix.ns.Database).C(ix.ns.Collection).EnsureIndex(index); err != nil {
			return "", fmt.Errorf("create primary union unique index failed[%v]", err)
		}
	}

	// create hash key only
	if err := ix.targetConn.Session.DB(ix.ns.Database).Run(bson.D{
		{Name: "createIndexes", Value: ix.ns.Collection},
		{Name: "indexes", Value: []bson.M{
			{
				"key": bson.M{
					primaryKeyWithType: "hashed",
				},
				"name": fmt.Sprintf("%s_%s", primaryKeyWithType, "hashed"),
			},
		}},
	}, nil); err != nil {
		return "", fmt.Errorf("create primary hash index failed[%v]", err)
	}

	return primaryKeyWithType, nil
}
