package writer

import (
	"testing"
	"fmt"
	"reflect"
	"nimo-shake/common"
	"nimo-shake/configure"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"nimo-shake/unit_test_common"
	"context"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
)

const (
	TestMongoAddressReplica  = unit_test_common.TestUrl
	TestMongoAddressSharding = unit_test_common.TestUrlSharding
	TestWriteDb              = "test_write_db"
	TestWriteTable           = "test_write_table"
	TestDynamoProxyAddress   = "http://dds-uf6e2095cd850034.mongodb.rds.aliyuncs.com:3717"
)

func TestMongo(t *testing.T) {
	var nr int

	utils.InitialLogger("", "debug", false)

	// test drop/create and simple CRUD
	{
		fmt.Printf("TestMongo case %d.\n", nr)
		nr++

		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.FullEnableIndexPrimary = true
		conf.Options.FullEnableIndexUser = true
		conf.Options.ConvertType = utils.ConvertTypeChange

		ns := utils.NS{Database: TestWriteDb, Collection: TestWriteTable}
		mongoWriter := NewWriter(utils.TargetTypeMongo, TestMongoAddressSharding, ns, "info")
		assert.Equal(t, true, mongoWriter != nil, "should be equal")

		// test connection
		mongoTestConn, err := utils.NewMongoConn(TestMongoAddressSharding, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")
		defer mongoTestConn.Close()

		// drop table
		err = mongoWriter.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		// check table exists
		exist, err := checkTableExist(mongoTestConn, TestWriteTable)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, exist, "should be equal")

		// create table with index
		err = mongoWriter.CreateTable(&dynamodb.TableDescription{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("pid"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("sid"),
					AttributeType: aws.String("N"),
				},
			},
			TableName: aws.String(TestWriteTable),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("pid"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("sid"),
					KeyType:       aws.String("RANGE"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")

		// check table exists
		exist, err = checkTableExist(mongoTestConn, TestWriteTable)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")

		// check index
		indexes, err := mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Indexes()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(indexes), "should be equal")

		// check index exists
		index1 := mgo.Index{
			Key:        []string{"$hashed:pid"},
			Background: true,
			Unique:     false,
			Name:       "pid_hashed",
		}
		exists := checkKeyExists(indexes, index1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exists, "should be equal")

		index2 := mgo.Index{
			Key:        []string{"pid", "sid"},
			Background: true,
			Unique:     true,
			Name:       "pid_1_sid_1",
		}
		exists = checkKeyExists(indexes, index2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exists, "should be equal")

		/************************************************/
		// test simple CRUD
		input := []interface{}{
			bson.M{
				"pid": "1",
				"sid": 100,
			},
		}
		index := []interface{}{
			bson.M{
				"pid": "1",
				"sid": 100,
			},
		}
		// insert
		err = mongoWriter.Insert(input, index)
		assert.Equal(t, nil, err, "should be equal")

		input = []interface{}{
			bson.M{
				"pid": "1",
				"sid": 1001,
			},
		}

		// update
		err = mongoWriter.Update(input, index)
		assert.Equal(t, nil, err, "should be equal")

		// query
		res := make(bson.M)
		err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(input[0]).One(&res)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "1", res["pid"], "should be equal")
		assert.Equal(t, 1001, res["sid"], "should be equal")

		// delete
		err = mongoWriter.Delete(input)
		assert.Equal(t, nil, err, "should be equal")

		// query
		cnt, err := mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(input[0]).Count()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, cnt, "should be equal")
	}

	// test drop/create and complicate CRUD
	{
		fmt.Printf("TestMongo case %d.\n", nr)
		nr++

		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.FullEnableIndexPrimary = true
		conf.Options.FullEnableIndexUser = true
		conf.Options.ConvertType = utils.ConvertTypeChange
		conf.Options.IncreaseExecutorUpsert = true
		conf.Options.IncreaseExecutorInsertOnDupUpdate = true

		ns := utils.NS{Database: TestWriteDb, Collection: TestWriteTable}
		mongoWriter := NewWriter(utils.TargetTypeMongo, TestMongoAddressSharding, ns, "info")
		assert.Equal(t, true, mongoWriter != nil, "should be equal")

		// test connection
		mongoTestConn, err := utils.NewMongoConn(TestMongoAddressSharding, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")
		defer mongoTestConn.Close()

		// drop table
		err = mongoWriter.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		// create table with index
		err = mongoWriter.CreateTable(&dynamodb.TableDescription{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("pid"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("sid"),
					AttributeType: aws.String("N"),
				},
			},
			TableName: aws.String(TestWriteTable),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("pid"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("sid"),
					KeyType:       aws.String("RANGE"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")

		/************************************************/
		// CRUD

		// insert 1-10
		input := make([]interface{}, 0, 10)
		index := make([]interface{}, 0, 10)
		for i := 1; i <= 10; i++ {
			pidS := fmt.Sprintf("%d", i)
			input = append(input, bson.M{
				"pid":  pidS,
				"sid":  i * 100,
				"data": i,
			})
			index = append(index, bson.M{
				"pid": pidS,
				"sid": i * 100,
			})
		}

		err = mongoWriter.Insert(input, index)
		assert.Equal(t, nil, err, "should be equal")

		// insert 5-15
		input2 := make([]interface{}, 0, 10)
		index2 := make([]interface{}, 0, 10)
		for i := 6; i <= 15; i++ {
			pidS := fmt.Sprintf("%d", i)
			input2 = append(input2, bson.M{
				"pid":  pidS,
				"sid":  i * 100,
				"data": i * 100,
			})
			index2 = append(index2, bson.M{
				"pid": pidS,
				"sid": i * 100,
			})
		}

		err = mongoWriter.Insert(input2, index2)
		assert.Equal(t, nil, err, "should be equal")

		// query count
		cnt, err := mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Count()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 15, cnt, "should be equal")

		// query
		data := make([]bson.M, 0)
		err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Sort("sid").All(&data)
		assert.Equal(t, nil, err, "should be equal")
		for i := 1; i <= 15; i++ {
			if i <= 5 {
				assert.Equal(t, i, data[i-1]["data"], "should be equal")
			} else {
				assert.Equal(t, i*100, data[i-1]["data"], "should be equal")
			}
		}

		// update 2-11
		input3 := make([]interface{}, 0, 10)
		index3 := make([]interface{}, 0, 10)
		for i := 2; i <= 11; i++ {
			pidS := fmt.Sprintf("%d", i)
			input3 = append(input3, bson.M{
				"pid":  pidS,
				"sid":  i * 100,
				"data": i * 1000,
			})
			index3 = append(index3, bson.M{
				"pid": pidS,
				"sid": i * 100,
			})
		}

		err = mongoWriter.Update(input3, index3)
		assert.Equal(t, nil, err, "should be equal")

		// query
		data = make([]bson.M, 0)
		err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Sort("sid").All(&data)
		assert.Equal(t, nil, err, "should be equal")
		for i := 1; i <= 15; i++ {
			if i < 2 {
				assert.Equal(t, i, data[i-1]["data"], "should be equal")
			} else if i <= 11 {
				assert.Equal(t, i*1000, data[i-1]["data"], "should be equal")
			} else {
				assert.Equal(t, i*100, data[i-1]["data"], "should be equal")
			}
		}

		// delete 10-20
		index4 := make([]interface{}, 0, 10)
		for i := 10; i <= 20; i++ {
			pidS := fmt.Sprintf("%d", i)
			index4 = append(index4, bson.M{
				"pid": pidS,
				"sid": i * 100,
			})
		}

		err = mongoWriter.Delete(index4)
		assert.Equal(t, nil, err, "should be equal")

		// query count
		cnt, err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Count()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 9, cnt, "should be equal")

		// query
		data = make([]bson.M, 0)
		err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Sort("sid").All(&data)
		assert.Equal(t, nil, err, "should be equal")

		for i := 1; i <= 9; i++ {
			if i < 2 {
				assert.Equal(t, i, data[i-1]["data"], "should be equal")
			} else if i <= 9 {
				assert.Equal(t, i*1000, data[i-1]["data"], "should be equal")
			}
		}

		// update 5-30
		input5 := make([]interface{}, 0, 10)
		index5 := make([]interface{}, 0, 10)
		for i := 5; i <= 30; i++ {
			pidS := fmt.Sprintf("%d", i)
			input5 = append(input5, bson.M{
				"pid":  pidS,
				"sid":  i * 100,
				"data": i * 10000,
			})
			index5 = append(index5, bson.M{
				"pid": pidS,
				"sid": i * 100,
			})
		}

		err = mongoWriter.Update(input5, index5)
		assert.Equal(t, nil, err, "should be equal")

		// query count
		cnt, err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Count()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 30, cnt, "should be equal")

		// query
		data = make([]bson.M, 0)
		err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Sort("sid").All(&data)
		assert.Equal(t, nil, err, "should be equal")

		for i := 1; i <= 30; i++ {
			if i < 2 {
				assert.Equal(t, i, data[i-1]["data"], "should be equal")
			} else if i < 5 {
				assert.Equal(t, i*1000, data[i-1]["data"], "should be equal")
			} else {
				assert.Equal(t, i*10000, data[i-1]["data"], "should be equal")
			}
		}
	}

	// test drop/create and bulk write
	{
		fmt.Printf("TestMongo case %d.\n", nr)
		nr++

		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.FullEnableIndexPrimary = true
		conf.Options.FullEnableIndexUser = true
		conf.Options.FullExecutorInsertOnDupUpdate = true
		conf.Options.ConvertType = utils.ConvertTypeChange
		conf.Options.IncreaseExecutorUpsert = true
		conf.Options.IncreaseExecutorInsertOnDupUpdate = true

		ns := utils.NS{Database: TestWriteDb, Collection: TestWriteTable}
		mongoWriter := NewWriter(utils.TargetTypeMongo, TestMongoAddressSharding, ns, "info")
		assert.Equal(t, true, mongoWriter != nil, "should be equal")

		// test connection
		mongoTestConn, err := utils.NewMongoConn(TestMongoAddressSharding, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")
		defer mongoTestConn.Close()

		// drop table
		err = mongoWriter.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		// create table with index
		err = mongoWriter.CreateTable(&dynamodb.TableDescription{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("pid"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("sid"),
					AttributeType: aws.String("N"),
				},
			},
			TableName: aws.String(TestWriteTable),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("pid"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("sid"),
					KeyType:       aws.String("RANGE"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")

		/************************************************/
		// CRUD

		// insert 1-10
		input := make([]interface{}, 0, 10)
		for i := 1; i <= 10; i++ {
			pidS := fmt.Sprintf("%d", i)
			input = append(input, bson.M{
				"pid":  pidS,
				"sid":  i * 100,
				"data": i,
			})
		}

		err = mongoWriter.WriteBulk(input)
		assert.Equal(t, nil, err, "should be equal")

		// query count
		cnt, err := mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Count()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 10, cnt, "should be equal")

		input2 := make([]interface{}, 0, 10)
		for i := 5; i <= 15; i++ {
			pidS := fmt.Sprintf("%d", i)
			input2 = append(input2, bson.M{
				"pid":  pidS,
				"sid":  i * 100,
				"data": i * 100,
			})
		}

		err = mongoWriter.WriteBulk(input2)
		assert.Equal(t, nil, err, "should be equal")

		// query count
		cnt, err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Count()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 15, cnt, "should be equal")

		// query
		data := make([]bson.M, 0)
		err = mongoTestConn.Session.DB(TestWriteDb).C(TestWriteTable).Find(bson.M{}).Sort("sid").All(&data)
		assert.Equal(t, nil, err, "should be equal")

		for i := 1; i <= 15; i++ {
			if i < 5 {
				assert.Equal(t, i, data[i-1]["data"], "should be equal")
			} else {
				assert.Equal(t, i*100, data[i-1]["data"], "should be equal")
			}
		}
	}
}

func TestMongoIndex(t *testing.T) {
	var nr int

	utils.InitialLogger("", "info", false)

	// sharding: pid + sid, primaryKey: false
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressSharding, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("sid"),
				KeyType:       aws.String("RANGE"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
			"sid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, false)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 3, len(res), "should be equal") // _id + 2
		assert.Equal(t, true, indexExists(res, "pid_1_sid_1", false), "should be equal")
		assert.Equal(t, true, indexExists(res, "pid_hashed", false), "should be equal")

		client.Close()
	}

	// sharding: pid + sid, primaryKey: true
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressSharding, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("sid"),
				KeyType:       aws.String("RANGE"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
			"sid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, true)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 3, len(res), "should be equal") // _id + 2
		assert.Equal(t, true, indexExists(res, "pid_1_sid_1", true), "should be equal")
		assert.Equal(t, true, indexExists(res, "pid_hashed", false), "should be equal")

		client.Close()
	}

	// sharding: pid, primaryKey: false
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressSharding, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, false)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 2, len(res), "should be equal") // _id + 1
		assert.Equal(t, true, indexExists(res, "pid_hashed", false), "should be equal")

		client.Close()
	}

	// sharding: pid, primaryKey: true
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressSharding, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, true)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 2, len(res), "should be equal") // _id + 1
		assert.Equal(t, true, indexExists(res, "pid_1", true), "should be equal")

		client.Close()
	}

	// replica: pid + sid, primaryKey: false
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeReplica
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressReplica, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("sid"),
				KeyType:       aws.String("RANGE"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
			"sid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, false)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 3, len(res), "should be equal") // _id + 2
		assert.Equal(t, true, indexExists(res, "pid_1_sid_1", false), "should be equal")
		assert.Equal(t, true, indexExists(res, "pid_1", false), "should be equal")

		client.Close()
	}

	// replica: pid + sid, primaryKey: true
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeReplica
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressReplica, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("sid"),
				KeyType:       aws.String("RANGE"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
			"sid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, true)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 3, len(res), "should be equal") // _id + 2
		assert.Equal(t, true, indexExists(res, "pid_1_sid_1", true), "should be equal")
		assert.Equal(t, true, indexExists(res, "pid_1", false), "should be equal")

		client.Close()
	}

	// replica: pid, primaryKey: false
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeReplica
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressReplica, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, false)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 2, len(res), "should be equal") // _id + 2
		assert.Equal(t, true, indexExists(res, "pid_1", false), "should be equal")


		client.Close()
	}

	// replica: pid, primaryKey: true
	{
		fmt.Printf("TestMongoIndex case %d.\n", nr)
		nr++

		conf.Options.TargetType = utils.TargetTypeMongo
		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeReplica
		conf.Options.ConvertType = utils.ConvertTypeChange

		client := NewMongoCommunityWriter("test_mongo_addr", TestMongoAddressReplica, utils.NS{
			Database:   TestWriteDb,
			Collection: TestWriteTable,
		})

		err := client.DropTable()
		assert.Equal(t, nil, err, "should be equal")

		keyEle := []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pid"),
				KeyType:       aws.String("HASH"),
			},
		}
		parseMap := map[string]string {
			"pid": "S",
		}
		_, err = client.createSingleIndex(keyEle, parseMap, true)
		assert.Equal(t, nil, err, "should be equal")

		res := make([]bson2.M, 0)
		cursor, err := client.conn.Client.Database(TestWriteDb).Collection(TestWriteTable).Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		err = cursor.All(context.Background(), &res)

		assert.Equal(t, 2, len(res), "should be equal") // _id + 2
		assert.Equal(t, true, indexExists(res, "pid_1", true), "should be equal")

		client.Close()
	}
}

func indexExists(ixes []bson2.M, name string, unique bool) bool {
	fmt.Println(ixes, name, unique)

	for _, ix := range ixes {
		if ix["name"] == name {
			ixUniq, ok := ix["unique"]
			if !ok {
				ixUniq = false
			}

			return ixUniq == unique
		}
	}
	return false
}

/*
func TestDynamo(t *testing.T) {
	var nr int

	utils.InitialLogger("", "", false)

	// test drop/create table and simple CRUD
	{
		fmt.Printf("TestDynamo case %d.\n", nr)
		nr++

		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.FullEnableIndexPrimary = true
		conf.Options.FullEnableIndexUser = true
		conf.Options.ConvertType = utils.ConvertTypeChange

		ns := utils.NS{Database: TestWriteDb, Collection: TestWriteTable}
		dynamoWriter := NewWriter(utils.TargetTypeAliyunDynamoProxy, TestDynamoProxyAddress, ns, "info")
		assert.Equal(t, true, dynamoWriter != nil, "should be equal")

		// test connection
		config := &aws.Config{
			Region:     aws.String("us-east-2"),
			Endpoint:   aws.String(TestDynamoProxyAddress),
			MaxRetries: aws.Int(3),
			HTTPClient: &http.Client{
				Timeout: time.Duration(5000) * time.Millisecond,
			},
		}

		var err error
		sess, err := session.NewSession(config)
		assert.Equal(t, nil, err, "should be equal")
		svc := dynamodb.New(sess, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))

		// drop table
		err = dynamoWriter.DropTable()
		// assert.Equal(t, true, err != nil, "should be equal")

		// check table exists
		_, err = svc.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(TestWriteTable),
		})
		assert.Equal(t, true, err != nil, "should be equal")
		fmt.Println(err)

		// create table with index
		inputTable := &dynamodb.TableDescription{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("pid"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("sid"),
					AttributeType: aws.String("N"),
				},
			},
			TableName: aws.String(TestWriteTable),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("pid"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("sid"),
					KeyType:       aws.String("RANGE"),
				},
			},
		}
		err = dynamoWriter.CreateTable(inputTable)
		assert.Equal(t, nil, err, "should be equal")

		// check table exists
		out, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(TestWriteTable),
		})
		assert.Equal(t, nil, err, "should be equal")

		fmt.Println(out)
		// check equal
		assert.Equal(t, inputTable.TableName, out.Table.TableName, "should be equal")
		assert.Equal(t, inputTable.KeySchema, out.Table.KeySchema, "should be equal")
		// assert.Equal(t, input.AttributeDefinitions, out.Table.AttributeDefinitions, "should be equal")

		dynamoWriter.Close()

		//
		// test simple CRUD
		input := []interface{}{
			map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String("1"),
				},
				"sid": {
					N: aws.String("100"),
				},
			},
		}
		index := []interface{}{
			map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String("1"),
				},
				"sid": {
					N: aws.String("100"),
				},
			},
		}
		// insert
		err = dynamoWriter.Insert(input, index)
		assert.Equal(t, nil, err, "should be equal")

		input = []interface{}{
			map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String("1"),
				},
				"sid": {
					N: aws.String("1001"),
				},
			},
		}

		// update
		err = dynamoWriter.Update(input, index)
		assert.Equal(t, nil, err, "should be equal")

		// query
		queryCondition := map[string]*dynamodb.AttributeValue{
			"pid": {
				S: aws.String("1"),
			},
			"sid": {
				N: aws.String("1001"),
			},
		}
		output, err := svc.Query(&dynamodb.QueryInput{
			TableName:              aws.String(TestWriteTable),
			KeyConditionExpression: aws.String("pid=:v1 AND sid=:v2"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v1": {
					S: aws.String("1"),
				},
				":v2": {
					N: aws.String("1001"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1, int(*output.Count), "should be equal")
		assert.Equal(t, queryCondition, output.Items[0], "should be equal")

		// delete
		err = dynamoWriter.Delete(input)
		assert.Equal(t, nil, err, "should be equal")

		// query again
		output, err = svc.Query(&dynamodb.QueryInput{
			TableName:              aws.String(TestWriteTable),
			KeyConditionExpression: aws.String("pid=:v1 AND sid=:v2"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v1": {
					S: aws.String("1"),
				},
				":v2": {
					N: aws.String("1001"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, int(*output.Count), "should be equal")
	}

	{
		fmt.Printf("TestDynamo case %d.\n", nr)
		nr++

		conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
		conf.Options.FullEnableIndexPrimary = true
		conf.Options.FullEnableIndexUser = true
		conf.Options.ConvertType = utils.ConvertTypeChange

		ns := utils.NS{Database: TestWriteDb, Collection: TestWriteTable}
		dynamoWriter := NewWriter(utils.TargetTypeAliyunDynamoProxy, TestDynamoProxyAddress, ns, "info")
		assert.Equal(t, true, dynamoWriter != nil, "should be equal")

		// test connection
		config := &aws.Config{
			Region:     aws.String("us-east-2"),
			Endpoint:   aws.String(TestDynamoProxyAddress),
			MaxRetries: aws.Int(3),
			HTTPClient: &http.Client{
				Timeout: time.Duration(5000) * time.Millisecond,
			},
		}

		var err error
		sess, err := session.NewSession(config)
		assert.Equal(t, nil, err, "should be equal")
		svc := dynamodb.New(sess, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))

		// drop table
		err = dynamoWriter.DropTable()
		// assert.Equal(t, true, err != nil, "should be equal")

		// check table exists
		_, err = svc.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(TestWriteTable),
		})
		assert.Equal(t, true, err != nil, "should be equal")
		fmt.Println(err)

		// create table with index
		inputTable := &dynamodb.TableDescription{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("pid"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("sid"),
					AttributeType: aws.String("N"),
				},
			},
			TableName: aws.String(TestWriteTable),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("pid"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("sid"),
					KeyType:       aws.String("RANGE"),
				},
			},
		}
		err = dynamoWriter.CreateTable(inputTable)
		assert.Equal(t, nil, err, "should be equal")

		// check table exists
		out, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(TestWriteTable),
		})
		assert.Equal(t, nil, err, "should be equal")

		fmt.Println(out)
		// check equal
		assert.Equal(t, inputTable.TableName, out.Table.TableName, "should be equal")
		assert.Equal(t, inputTable.KeySchema, out.Table.KeySchema, "should be equal")
		// assert.Equal(t, input.AttributeDefinitions, out.Table.AttributeDefinitions, "should be equal")

		dynamoWriter.Close()

		// CRUD

		// insert 1-10
		input := make([]interface{}, 0, 10)
		index := make([]interface{}, 0, 10)
		for i := 1; i <= 10; i++ {
			pidS := fmt.Sprintf("%d", 1)
			sidN := fmt.Sprintf("%d", i*100)
			input = append(input, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
			})
			index = append(index, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
			})
		}
		// insert
		err = dynamoWriter.Insert(input, index)
		assert.Equal(t, nil, err, "should be equal")

		// insert 5-15
		input2 := make([]interface{}, 0, 10)
		index2 := make([]interface{}, 0, 10)
		for i := 6; i <= 15; i++ {
			pidS := fmt.Sprintf("%d", 1)
			sidN := fmt.Sprintf("%d", i*100)
			dataN := fmt.Sprintf("%d", i*100)
			input2 = append(input2, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
				"data": {
					N: aws.String(dataN),
				},
			})
			index2 = append(index2, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
			})
		}
		// insert
		err = dynamoWriter.Insert(input2, index2)
		assert.Equal(t, nil, err, "should be equal")

		// query
		output, err := svc.Query(&dynamodb.QueryInput{
			TableName:              aws.String(TestWriteTable),
			KeyConditionExpression: aws.String("pid=:v1"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v1": {
					S: aws.String("1"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 15, int(*output.Count), "should be equal")
		for i := range output.Items {
			ele := output.Items[i]
			data, exist := ele["data"]
			sidValue, _ := strconv.Atoi(*ele["sid"].N)
			if sidValue <= 500 {
				assert.Equal(t, false, exist, "should be equal")
			} else {
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, *ele["sid"].N, *data.N, "should be equal")
			}
		}

		// update 2-11
		input3 := make([]interface{}, 0, 10)
		index3 := make([]interface{}, 0, 10)
		for i := 2; i <= 11; i++ {
			pidS := fmt.Sprintf("%d", 1)
			sidN := fmt.Sprintf("%d", i*100)
			dataN := fmt.Sprintf("%d", i*1000)
			input3 = append(input3, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
				"data": {
					N: aws.String(dataN),
				},
			})
			index3 = append(index3, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
			})
		}

		err = dynamoWriter.Update(input3, index3)
		assert.Equal(t, nil, err, "should be equal")

		// query
		output, err = svc.Query(&dynamodb.QueryInput{
			TableName:              aws.String(TestWriteTable),
			KeyConditionExpression: aws.String("pid=:v1"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v1": {
					S: aws.String("1"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 15, int(*output.Count), "should be equal")
		for i := range output.Items {
			ele := output.Items[i]
			data, exist := ele["data"]
			sidValue, _ := strconv.Atoi(*ele["sid"].N)
			if sidValue <= 100 {
				assert.Equal(t, false, exist, "should be equal")
			} else if sidValue <= 1100 {
				dataValue, _ := strconv.Atoi(*ele["data"].N)
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, sidValue*10, dataValue, "should be equal")
			} else {
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, *ele["sid"].N, *data.N, "should be equal")
			}
		}

		// delete 10-20
		index4 := make([]interface{}, 0, 10)
		for i := 10; i <= 20; i++ {
			pidS := fmt.Sprintf("%d", 1)
			sidN := fmt.Sprintf("%d", i*100)
			index4 = append(index4, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
			})
		}

		err = dynamoWriter.Delete(index4)
		assert.Equal(t, nil, err, "should be equal")

		// query
		output, err = svc.Query(&dynamodb.QueryInput{
			TableName:              aws.String(TestWriteTable),
			KeyConditionExpression: aws.String("pid=:v1"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v1": {
					S: aws.String("1"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 9, int(*output.Count), "should be equal")
		for i := range output.Items {
			ele := output.Items[i]
			data, exist := ele["data"]
			sidValue, _ := strconv.Atoi(*ele["sid"].N)
			if sidValue <= 100 {
				assert.Equal(t, false, exist, "should be equal")
			} else if sidValue <= 1100 {
				dataValue, _ := strconv.Atoi(*ele["data"].N)
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, sidValue*10, dataValue, "should be equal")
			} else {
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, *ele["sid"].N, *data.N, "should be equal")
			}
		}

		// update 5-30
		input5 := make([]interface{}, 0, 10)
		index5 := make([]interface{}, 0, 10)
		for i := 5; i <= 30; i++ {
			pidS := fmt.Sprintf("%d", 1)
			sidN := fmt.Sprintf("%d", i*100)
			dataN := fmt.Sprintf("%d", i*10000)
			input5 = append(input5, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
				"data": {
					N: aws.String(dataN),
				},
			})
			index5 = append(index5, map[string]*dynamodb.AttributeValue{
				"pid": {
					S: aws.String(pidS),
				},
				"sid": {
					N: aws.String(sidN),
				},
			})
		}

		err = dynamoWriter.Update(input5, index5)
		assert.Equal(t, nil, err, "should be equal")

		// query
		output, err = svc.Query(&dynamodb.QueryInput{
			TableName:              aws.String(TestWriteTable),
			KeyConditionExpression: aws.String("pid=:v1"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v1": {
					S: aws.String("1"),
				},
			},
		})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 30, int(*output.Count), "should be equal")
		for i := range output.Items {
			ele := output.Items[i]
			_, exist := ele["data"]
			sidValue, _ := strconv.Atoi(*ele["sid"].N)
			if sidValue <= 100 {
				assert.Equal(t, false, exist, "should be equal")
			} else if sidValue <= 400 {
				dataValue, _ := strconv.Atoi(*ele["data"].N)
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, sidValue*10, dataValue, "should be equal")
			} else {
				dataValue, _ := strconv.Atoi(*ele["data"].N)
				assert.Equal(t, true, exist, "should be equal")
				assert.Equal(t, sidValue*100, dataValue, "should be equal")
			}
		}
	}

	// test bulkWrite
	{
		{
			fmt.Printf("TestDynamo case %d.\n", nr)
			nr++

			conf.Options.TargetMongoDBType = utils.TargetMongoDBTypeSharding
			conf.Options.FullEnableIndexPrimary = true
			conf.Options.FullEnableIndexUser = true
			conf.Options.ConvertType = utils.ConvertTypeChange

			ns := utils.NS{Database: TestWriteDb, Collection: TestWriteTable}
			dynamoWriter := NewWriter(utils.TargetTypeAliyunDynamoProxy, TestDynamoProxyAddress, ns, "info")
			assert.Equal(t, true, dynamoWriter != nil, "should be equal")

			// test connection
			config := &aws.Config{
				Region:     aws.String("us-east-2"),
				Endpoint:   aws.String(TestDynamoProxyAddress),
				MaxRetries: aws.Int(3),
				HTTPClient: &http.Client{
					Timeout: time.Duration(5000) * time.Millisecond,
				},
			}

			var err error
			sess, err := session.NewSession(config)
			assert.Equal(t, nil, err, "should be equal")
			svc := dynamodb.New(sess, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))

			// drop table
			err = dynamoWriter.DropTable()
			// assert.Equal(t, true, err != nil, "should be equal")

			// check table exists
			_, err = svc.DescribeTable(&dynamodb.DescribeTableInput{
				TableName: aws.String(TestWriteTable),
			})
			assert.Equal(t, true, err != nil, "should be equal")
			fmt.Println(err)

			// create table with index
			inputTable := &dynamodb.TableDescription{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String("pid"),
						AttributeType: aws.String("S"),
					},
					{
						AttributeName: aws.String("sid"),
						AttributeType: aws.String("N"),
					},
				},
				TableName: aws.String(TestWriteTable),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("pid"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("sid"),
						KeyType:       aws.String("RANGE"),
					},
				},
			}
			err = dynamoWriter.CreateTable(inputTable)
			assert.Equal(t, nil, err, "should be equal")

			// check table exists
			out, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
				TableName: aws.String(TestWriteTable),
			})
			assert.Equal(t, nil, err, "should be equal")

			fmt.Println(out)
			// check equal
			assert.Equal(t, inputTable.TableName, out.Table.TableName, "should be equal")
			assert.Equal(t, inputTable.KeySchema, out.Table.KeySchema, "should be equal")
			// assert.Equal(t, input.AttributeDefinitions, out.Table.AttributeDefinitions, "should be equal")

			dynamoWriter.Close()

			// CRUD

			// insert 1-10
			input := make([]interface{}, 0, 10)
			for i := 1; i <= 10; i++ {
				pidS := fmt.Sprintf("%d", 1)
				sidN := fmt.Sprintf("%d", i*100)
				input = append(input, map[string]*dynamodb.AttributeValue{
					"pid": {
						S: aws.String(pidS),
					},
					"sid": {
						N: aws.String(sidN),
					},
				})
			}
			// insert
			err = dynamoWriter.WriteBulk(input)
			assert.Equal(t, nil, err, "should be equal")

			// insert 5-15
			input2 := make([]interface{}, 0, 10)
			for i := 5; i <= 15; i++ {
				pidS := fmt.Sprintf("%d", 1)
				sidN := fmt.Sprintf("%d", i*100)
				dataN := fmt.Sprintf("%d", i*100)
				input2 = append(input2, map[string]*dynamodb.AttributeValue{
					"pid": {
						S: aws.String(pidS),
					},
					"sid": {
						N: aws.String(sidN),
					},
					"data": {
						N: aws.String(dataN),
					},
				})
			}
			// insert
			err = dynamoWriter.WriteBulk(input2)
			assert.Equal(t, nil, err, "should be equal")

			// query
			output, err := svc.Query(&dynamodb.QueryInput{
				TableName:              aws.String(TestWriteTable),
				KeyConditionExpression: aws.String("pid=:v1"),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":v1": {
						S: aws.String("1"),
					},
				},
			})
			assert.Equal(t, nil, err, "should be equal")
			assert.Equal(t, 15, int(*output.Count), "should be equal")
			for i := range output.Items {
				ele := output.Items[i]
				data, exist := ele["data"]
				sidValue, _ := strconv.Atoi(*ele["sid"].N)
				if sidValue < 500 {
					assert.Equal(t, false, exist, "should be equal")
				} else {
					assert.Equal(t, true, exist, "should be equal")
					assert.Equal(t, *ele["sid"].N, *data.N, "should be equal")
				}
			}
		}
	}
}*/

func checkTableExist(conn *utils.MongoConn, table string) (bool, error) {
	list, err := conn.Session.DB(TestWriteDb).CollectionNames()
	if err != nil {
		return false, err
	}

	for _, ele := range list {
		if ele == table {
			return true, nil
		}
	}
	return false, nil
}

func checkKeyExists(indexList []mgo.Index, index mgo.Index) bool {
	for _, ele := range indexList {
		if ele.Name == index.Name {
			return reflect.DeepEqual(ele.Key, index.Key) && ele.Background == index.Background && ele.Unique == index.Unique
		}
	}
	return false
}
