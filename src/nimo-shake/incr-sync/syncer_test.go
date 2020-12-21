package incr_sync

import (
	"testing"
	"time"
	"fmt"

	"nimo-shake/common"
	"nimo-shake/checkpoint"
	"nimo-shake/protocal"
	"nimo-shake/configure"
	"nimo-shake/writer"

	"github.com/stretchr/testify/assert"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/vinllen/mgo/bson"
	"github.com/vinllen/mgo"
)

const (
	TestMongoAddress = "mongodb://100.81.164.186:31772,100.81.164.186:31771,100.81.164.186:31773"
)

func TestBatcher(t *testing.T) {
	// test batcher
	var nr int

	conf.Options.TargetType = utils.TargetTypeMongo
	conf.Options.TargetAddress = TestMongoAddress
	conf.Options.LogLevel = "info"

	// simple test
	{
		fmt.Printf("TestBatcher case %d.\n", nr)
		nr++

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			unitTestStr:  "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		go d.batcher()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}

		node := <-d.executorChan
		// fmt.Println(node)
		assert.Equal(t, EventInsert, node.tp, "should be equal")
		assert.Equal(t, "100", node.lastSequenceNumber, "should be equal")
		assert.Equal(t, 1, len(node.operate), "should be equal")

		timeout := false
		select {
		case node = <-d.executorChan:
		case <-time.After(time.Second * 5):
			timeout = true
		}
		assert.Equal(t, true, timeout, "should be equal")
	}

	// test different operate
	{
		fmt.Printf("TestBatcher case %d.\n", nr)
		nr++

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			unitTestStr:  "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		go d.batcher()
		// put 1
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// put 2
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("101"),
			},
		}
		// remove 3
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventRemove),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value2"),
					},
				},
				OldImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}

		node := <-d.executorChan
		fmt.Println(node)
		assert.Equal(t, EventInsert, node.tp, "should be equal")
		assert.Equal(t, "101", node.lastSequenceNumber, "should be equal")
		assert.Equal(t, 2, len(node.index), "should be equal")
		assert.Equal(t, 2, len(node.operate), "should be equal")

		node = <-d.executorChan
		fmt.Println(node)
		assert.Equal(t, EventRemove, node.tp, "should be equal")
		assert.Equal(t, "102", node.lastSequenceNumber, "should be equal")
		assert.Equal(t, 1, len(node.index), "should be equal")
		assert.Equal(t, 0, len(node.operate), "should be equal")

		timeout := false
		select {
		case node = <-d.executorChan:
		case <-time.After(time.Second * 5):
			timeout = true
		}
		assert.Equal(t, true, timeout, "should be equal")
	}

	// overpass batch number
	{
		fmt.Printf("TestBatcher case %d.\n", nr)
		nr++

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			unitTestStr:  "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		BatcherNumber = 3

		go d.batcher()
		// put 1
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// put 2
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value2"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("101"),
			},
		}
		// put 3
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value3"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value3"),
					},
					"test3": {
						S: aws.String("value3"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}
		// put 3
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value4"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value4"),
					},
					"test4": {
						S: aws.String("value4"),
					},
				},
				SequenceNumber: aws.String("103"),
			},
		}

		node := <-d.executorChan
		// fmt.Println(node)
		assert.Equal(t, EventInsert, node.tp, "should be equal")
		assert.Equal(t, "102", node.lastSequenceNumber, "should be equal")
		assert.Equal(t, 3, len(node.operate), "should be equal")
		assert.Equal(t, 3, len(node.index), "should be equal")

		node = <-d.executorChan
		// fmt.Println(node)
		assert.Equal(t, EventInsert, node.tp, "should be equal")
		assert.Equal(t, "103", node.lastSequenceNumber, "should be equal")
		assert.Equal(t, 1, len(node.operate), "should be equal")
		assert.Equal(t, 1, len(node.index), "should be equal")

		timeout := false
		select {
		case node = <-d.executorChan:
		case <-time.After(time.Second * 5):
			timeout = true
		}
		assert.Equal(t, true, timeout, "should be equal")
	}

	// test timeout
	{
		fmt.Printf("TestBatcher case %d.\n", nr)
		nr++

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			unitTestStr:  "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		BatcherNumber = 100

		go d.batcher()
		// put 1
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String("key_value"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("105"),
			},
		}

		var node *ExecuteNode
		timeout := false
		select {
		case node = <-d.executorChan:
		case <-time.After(time.Second * 5):
			timeout = true
		}
		assert.Equal(t, false, timeout, "should be equal")
		// fmt.Println(node)
		assert.Equal(t, EventInsert, node.tp, "should be equal")
		assert.Equal(t, "105", node.lastSequenceNumber, "should be equal")
		assert.Equal(t, 1, len(node.operate), "should be equal")
		assert.Equal(t, 1, len(node.index), "should be equal")
	}
}

func TestBatcher_Executor(t *testing.T) {
	// test batcher and executor
	var nr int

	conf.Options.TargetType = utils.TargetTypeMongo
	conf.Options.TargetAddress = TestMongoAddress
	conf.Options.LogLevel = "info"

	// simple test
	{
		fmt.Printf("TestBatcher_Executor case %d.\n", nr)
		nr++

		utils.InitialLogger("", "info", true)

		ckpt := checkpoint.NewWriter(checkpoint.CheckpointWriterTypeFile, "test-ckpt", "test-db")
		// remove test checkpoint table
		err := ckpt.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		targetClient, err := utils.NewMongoConn(TestMongoAddress, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			ns: utils.NS{
				Database:   "utTestDB",
				Collection: "utTestCollection",
			},
			unitTestStr: "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		go d.batcher()
		go d.executor()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}

		// wait executor run
		time.Sleep(3 * time.Second)

		assert.Equal(t, "100", d.checkpointPosition, "should be equal")

		// output := make(bson.M)
		var output bson.M
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value0"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value0",
			},
			"test1": bson.M{
				"S": "value1",
			},
			"test2": bson.M{
				"N": "123",
			},
		}, output, "should be equal")
	}

	{
		fmt.Printf("TestBatcher_Executor case %d.\n", nr)
		nr++

		utils.InitialLogger("", "info", true)

		ckpt := checkpoint.NewWriter(checkpoint.CheckpointWriterTypeFile, "test-ckpt", "test-db")
		// remove test checkpoint table
		err := ckpt.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		targetClient, err := utils.NewMongoConn(TestMongoAddress, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			ns: utils.NS{
				Database:   "utTestDB",
				Collection: "utTestCollection",
			},
			unitTestStr: "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		go d.batcher()
		go d.executor()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// put 2
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key2": {
						S: aws.String("key_value2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key2": {
						S: aws.String("key_value2"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("101"),
			},
		}
		// remove 3
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventRemove),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key2": {
						S: aws.String("key_value2"),
					},
				},
				OldImage: map[string]*dynamodb.AttributeValue{
					"key2": {
						S: aws.String("key_value2"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}

		// wait executor run
		time.Sleep(3 * time.Second)

		assert.Equal(t, "102", d.checkpointPosition, "should be equal")

		// output := make(bson.M)
		var output bson.M
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value0"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value0",
			},
			"test1": bson.M{
				"S": "value1",
			},
			"test2": bson.M{
				"N": "123",
			},
		}, output, "should be equal")

		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key2.S": "key_value2"}).One(&output)
		assert.Equal(t, utils.NotFountErr, err.Error(), "should be equal")
	}

	{
		fmt.Printf("TestBatcher_Executor case %d.\n", nr)
		nr++

		utils.InitialLogger("", "info", true)

		ckpt := checkpoint.NewWriter(checkpoint.CheckpointWriterTypeFile, "test-ckpt", "test-db")
		// remove test checkpoint table
		err := ckpt.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		targetClient, err := utils.NewMongoConn(TestMongoAddress, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			ns: utils.NS{
				Database:   "utTestDB",
				Collection: "utTestCollection",
			},
			unitTestStr: "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)
		d.targetWriter.DropTable()

		go d.batcher()
		go d.executor()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// put 2
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key2": {
						S: aws.String("key_value2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key2": {
						S: aws.String("key_value2"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("101"),
			},
		}
		// put 3
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key3": {
						S: aws.String("key_value3"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key3": {
						S: aws.String("key_value3"),
					},
					"test3": {
						S: aws.String("value3"),
					},
				},
				SequenceNumber: aws.String("105"),
			},
		}

		// wait executor run
		time.Sleep(3 * time.Second)

		assert.Equal(t, "105", d.checkpointPosition, "should be equal")

		// output := make(bson.M)
		var output bson.M
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value0"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value0",
			},
			"test1": bson.M{
				"S": "value1",
			},
			"test2": bson.M{
				"N": "123",
			},
		}, output, "should be equal")

		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key3.S": "key_value3"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key3": bson.M{
				"S": "key_value3",
			},
			"test3": bson.M{
				"S": "value3",
			},
		}, output, "should be equal")
	}

	// test error in bulk
	{
		fmt.Printf("TestBatcher_Executor case %d.\n", nr)
		nr++

		conf.Options.IncreaseExecutorInsertOnDupUpdate = true

		utils.InitialLogger("", "info", true)

		ckpt := checkpoint.NewWriter(checkpoint.CheckpointWriterTypeFile, "test-ckpt", "test-db")
		// remove test checkpoint table
		err := ckpt.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		targetClient, err := utils.NewMongoConn(TestMongoAddress, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")
		targetClient.Session.DB("utTestDB").C("utTestCollection").DropCollection()

		// create index
		err = targetClient.Session.DB("utTestDB").C("utTestCollection").EnsureIndex(mgo.Index{
			Key: []string{"key0"},
			Unique: true,
		})
		assert.Equal(t, nil, err, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			ns: utils.NS{
				Database:   "utTestDB",
				Collection: "utTestCollection",
			},
			unitTestStr: "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)

		go d.batcher()
		go d.executor()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// normal key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value1"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value1"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}
		// duplicate key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("124"),
					},
				},
				SequenceNumber: aws.String("103"),
			},
		}

		// wait executor run
		time.Sleep(3 * time.Second)

		assert.Equal(t, "103", d.checkpointPosition, "should be equal")

		// output := make(bson.M)
		var output bson.M
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value0"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value0",
			},
			"test1": bson.M{
				"S": "value1",
			},
			"test2": bson.M{
				"N": "124",
			},
		}, output, "should be equal")
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value1"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value1",
			},
			"test2": bson.M{
				"S": "value2",
			},
		}, output, "should be equal")
	}

	// test error in bulk
	{
		fmt.Printf("TestBatcher_Executor case %d.\n", nr)
		nr++

		utils.InitialLogger("", "info", true)

		ckpt := checkpoint.NewWriter(checkpoint.CheckpointWriterTypeFile, "test-ckpt", "test-db")
		// remove test checkpoint table
		err := ckpt.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		targetClient, err := utils.NewMongoConn(TestMongoAddress, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")
		targetClient.Session.DB("utTestDB").C("utTestCollection").DropCollection()

		// create index
		err = targetClient.Session.DB("utTestDB").C("utTestCollection").EnsureIndex(mgo.Index{
			Key:    []string{"key0"},
			Unique: true,
		})
		assert.Equal(t, nil, err, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			ns: utils.NS{
				Database:   "utTestDB",
				Collection: "utTestCollection",
			},
			unitTestStr: "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)

		go d.batcher()
		go d.executor()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// normal key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value1"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value1"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}
		// duplicate key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventMODIFY),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value1"),
					},
				},
				OldImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value1"),
					},
					"test2": {
						S: aws.String("value2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("key_value0"),
					},
					"test1": {
						S: aws.String("value1"),
					},
					"test2": {
						N: aws.String("123"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}

		// wait executor run
		time.Sleep(3 * time.Second)

		assert.Equal(t, "102", d.checkpointPosition, "should be equal")

		// output := make(bson.M)
		var output bson.M
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value0"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value0",
			},
			"test1": bson.M{
				"S": "value1",
			},
			"test2": bson.M{
				"N": "123",
			},
		}, output, "should be equal")
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "key_value1"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "key_value1",
			},
			"test2": bson.M{
				"S": "value2",
			},
		}, output, "should be equal")
	}

	// test error in bulk
	{
		fmt.Printf("TestBatcher_Executor case %d.\n", nr)
		nr++

		utils.InitialLogger("", "info", true)

		ckpt := checkpoint.NewWriter(checkpoint.CheckpointWriterTypeFile, "test-ckpt", "test-db")
		// remove test checkpoint table
		err := ckpt.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		converter := protocal.NewConverter("raw")
		assert.Equal(t, true, converter != nil, "should be equal")

		targetClient, err := utils.NewMongoConn(TestMongoAddress, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")
		targetClient.Session.DB("utTestDB").C("utTestCollection").DropCollection()

		// create index
		err = targetClient.Session.DB("utTestDB").C("utTestCollection").EnsureIndex(mgo.Index{
			Key:    []string{"key0"},
			Unique: true,
		})
		assert.Equal(t, nil, err, "should be equal")

		d := &Dispatcher{
			batchChan:    make(chan *dynamodbstreams.Record, 100),
			executorChan: make(chan *ExecuteNode, 100),
			converter:    converter,
			ns: utils.NS{
				Database:   "utTestDB",
				Collection: "utTestCollection",
			},
			unitTestStr: "test",
		}
		d.targetWriter = writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, d.ns, conf.Options.LogLevel)

		go d.batcher()
		go d.executor()

		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v0"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v0"),
					},
				},
				SequenceNumber: aws.String("100"),
			},
		}
		// normal key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v1"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v1"),
					},
				},
				SequenceNumber: aws.String("102"),
			},
		}
		// normal key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v2"),
					},
				},
				SequenceNumber: aws.String("103"),
			},
		}
		// duplicate key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventMODIFY),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v2"),
					},
				},
				OldImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v2"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v1"),
					},
				},
				SequenceNumber: aws.String("104"),
			},
		}
		// normal key
		d.batchChan <- &dynamodbstreams.Record{
			EventName: aws.String(EventInsert),
			Dynamodb: &dynamodbstreams.StreamRecord{
				Keys: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v3"),
					},
				},
				NewImage: map[string]*dynamodb.AttributeValue{
					"key0": {
						S: aws.String("v3"),
					},
				},
				SequenceNumber: aws.String("105"),
			},
		}

		// wait executor run
		time.Sleep(3 * time.Second)

		assert.Equal(t, "105", d.checkpointPosition, "should be equal")

		// output := make(bson.M)
		var output bson.M
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "v0"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "v0",
			},
		}, output, "should be equal")
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "v1"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "v1",
			},
		}, output, "should be equal")
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "v2"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "v2",
			},
		}, output, "should be equal")
		err = targetClient.Session.DB(d.ns.Database).C(d.ns.Collection).Find(bson.M{"key0.S": "v3"}).One(&output)
		// fmt.Println(output)
		delete(output, "_id")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"key0": bson.M{
				"S": "v3",
			},
		}, output, "should be equal")
	}
}
