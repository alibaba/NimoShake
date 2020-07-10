package checkpoint

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"

	"nimo-shake/common"
)

const (
	TestMongoAddress    = "mongodb://100.81.164.186:31883"
	TestCheckpointDb    = "test_checkpoint_db"
	TestCheckpointTable = "test_checkpoint_table"
)

func TestStatus(t *testing.T) {
	var err error
	mongoWriter := NewWriter(CheckpointWriterTypeMongo, TestMongoAddress, TestCheckpointDb)
	assert.Equal(t, true, mongoWriter != nil, "should be equal")

	fileWriter := NewWriter(CheckpointWriterTypeFile, TestMongoAddress, TestCheckpointDb)
	assert.Equal(t, true, fileWriter != nil, "should be equal")

	utils.InitialLogger("", "debug", false)

	var nr int
	// test status: mongo
	{
		fmt.Printf("TestStatus case %d.\n", nr)
		nr++

		err = mongoWriter.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		status, err := mongoWriter.FindStatus()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, CheckpointStatusValueEmpty, status, "should be equal")

		err = mongoWriter.UpdateStatus(CheckpointStatusValueIncrSync)
		assert.Equal(t, nil, err, "should be equal")

		status, err = mongoWriter.FindStatus()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, CheckpointStatusValueIncrSync, status, "should be equal")
	}

	// test status: file
	{
		fmt.Printf("TestStatus case %d.\n", nr)
		nr++

		err = fileWriter.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		status, err := fileWriter.FindStatus()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, CheckpointStatusValueEmpty, status, "should be equal")

		err = fileWriter.UpdateStatus(CheckpointStatusValueIncrSync)
		assert.Equal(t, nil, err, "should be equal")

		status, err = fileWriter.FindStatus()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, CheckpointStatusValueIncrSync, status, "should be equal")
	}
}

func TestCheckpointCRUD(t *testing.T) {
	var err error
	mongoWriter := NewWriter(CheckpointWriterTypeMongo, TestMongoAddress, TestCheckpointDb)
	assert.Equal(t, true, mongoWriter != nil, "should be equal")

	fileWriter := NewWriter(CheckpointWriterTypeFile, TestMongoAddress, TestCheckpointDb)
	assert.Equal(t, true, fileWriter != nil, "should be equal")

	// utils.InitialLogger("", "info", false)

	var nr int
	// test CRUD: mongo
	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		err = mongoWriter.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		cpkt := &Checkpoint{
			ShardId:  "test_id",
			FatherId: "test_father",
			Status:   StatusNotProcess,
		}

		err = mongoWriter.Update("test_id", cpkt, TestCheckpointTable)
		assert.Equal(t, utils.NotFountErr, err.Error(), "should be equal")

		err = mongoWriter.UpdateWithSet("test_id", map[string]interface{}{
			"Status": StatusNotProcess,
		}, TestCheckpointTable)
		assert.Equal(t, utils.NotFountErr, err.Error(), "should be equal")

		err = mongoWriter.Insert(cpkt, TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")

		ckptRet, err := mongoWriter.Query("test_id", TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, cpkt.ShardId, ckptRet.ShardId, "should be equal")
		assert.Equal(t, cpkt.FatherId, ckptRet.FatherId, "should be equal")
		assert.Equal(t, cpkt.Status, ckptRet.Status, "should be equal")

		err = mongoWriter.UpdateWithSet("test_id", map[string]interface{}{
			"Status": StatusInProcessing,
		}, TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")

		ckptRet, err = mongoWriter.Query("test_id", TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, cpkt.ShardId, ckptRet.ShardId, "should be equal")
		assert.Equal(t, cpkt.FatherId, ckptRet.FatherId, "should be equal")
		assert.Equal(t, StatusInProcessing, ckptRet.Status, "should be equal")
	}

	// test CRUD: file
	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		err = fileWriter.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		cpkt := &Checkpoint{
			ShardId:  "test_id",
			FatherId: "test_father",
			Status:   StatusNotProcess,
		}

		err = fileWriter.Update("test_id", cpkt, TestCheckpointTable)
		assert.Equal(t, true, err != nil, "should be equal")
		fmt.Println(err)

		err = fileWriter.UpdateWithSet("test_id", map[string]interface{}{
			"Status": StatusNotProcess,
		}, TestCheckpointTable)
		assert.Equal(t, true, err != nil, "should be equal")
		fmt.Println(err)

		err = fileWriter.Insert(cpkt, TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")

		ckptRet, err := fileWriter.Query("test_id", TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, cpkt.ShardId, ckptRet.ShardId, "should be equal")
		assert.Equal(t, cpkt.FatherId, ckptRet.FatherId, "should be equal")
		assert.Equal(t, cpkt.Status, ckptRet.Status, "should be equal")

		err = fileWriter.UpdateWithSet("test_id", map[string]interface{}{
			"Status": StatusInProcessing,
		}, TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")

		ckptRet, err = fileWriter.Query("test_id", TestCheckpointTable)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, cpkt.ShardId, ckptRet.ShardId, "should be equal")
		assert.Equal(t, cpkt.FatherId, ckptRet.FatherId, "should be equal")
		assert.Equal(t, StatusInProcessing, ckptRet.Status, "should be equal")
	}
}

func TestExtractCheckpoint(t *testing.T) {
	var err error
	mongoWriter := NewWriter(CheckpointWriterTypeMongo, TestMongoAddress, TestCheckpointDb)
	assert.Equal(t, true, mongoWriter != nil, "should be equal")

	fileWriter := NewWriter(CheckpointWriterTypeFile, TestMongoAddress, TestCheckpointDb)
	assert.Equal(t, true, fileWriter != nil, "should be equal")

	// utils.InitialLogger("", "info", false)

	var nr int
	// test CRUD: mongo
	{
		fmt.Printf("TestExtractCheckpoint case %d.\n", nr)
		nr++

		err = mongoWriter.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		err = mongoWriter.Insert(&Checkpoint{
			ShardId: "id1",
			Status:  StatusNotProcess,
		}, "table1")
		assert.Equal(t, nil, err, "should be equal")

		err = mongoWriter.Insert(&Checkpoint{
			ShardId: "id2",
			Status:  StatusInProcessing,
		}, "table1")
		assert.Equal(t, nil, err, "should be equal")

		err = mongoWriter.Insert(&Checkpoint{
			ShardId: "id3",
			Status:  StatusPrepareProcess,
		}, "table1")
		assert.Equal(t, nil, err, "should be equal")

		err = mongoWriter.Insert(&Checkpoint{
			ShardId: "id1",
			Status:  StatusDone,
		}, "table2")
		assert.Equal(t, nil, err, "should be equal")

		err = mongoWriter.Insert(&Checkpoint{
			ShardId: "id10",
			Status:  StatusWaitFather,
		}, "table2")
		assert.Equal(t, nil, err, "should be equal")

		mp, err := mongoWriter.ExtractCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(mp), "should be equal")
		assert.Equal(t, 3, len(mp["table1"]), "should be equal")
		assert.Equal(t, 2, len(mp["table2"]), "should be equal")
		assert.Equal(t, StatusNotProcess, mp["table1"]["id1"].Status, "should be equal")
		assert.Equal(t, StatusInProcessing, mp["table1"]["id2"].Status, "should be equal")
		assert.Equal(t, StatusPrepareProcess, mp["table1"]["id3"].Status, "should be equal")
		assert.Equal(t, StatusDone, mp["table2"]["id1"].Status, "should be equal")
		assert.Equal(t, StatusWaitFather, mp["table2"]["id10"].Status, "should be equal")
	}

	// test CRUD: file
	{
		fmt.Printf("TestExtractCheckpoint case %d.\n", nr)
		nr++

		err = fileWriter.DropAll()
		assert.Equal(t, nil, err, "should be equal")

		err = fileWriter.Insert(&Checkpoint{
			ShardId: "id1",
			Status:  StatusNotProcess,
		}, "table1")
		assert.Equal(t, nil, err, "should be equal")

		err = fileWriter.Insert(&Checkpoint{
			ShardId: "id2",
			Status:  StatusInProcessing,
		}, "table1")
		assert.Equal(t, nil, err, "should be equal")

		err = fileWriter.Insert(&Checkpoint{
			ShardId: "id3",
			Status:  StatusPrepareProcess,
		}, "table1")
		assert.Equal(t, nil, err, "should be equal")

		err = fileWriter.Insert(&Checkpoint{
			ShardId: "id1",
			Status:  StatusDone,
		}, "table2")
		assert.Equal(t, nil, err, "should be equal")

		err = fileWriter.Insert(&Checkpoint{
			ShardId: "id10",
			Status:  StatusWaitFather,
		}, "table2")
		assert.Equal(t, nil, err, "should be equal")

		mp, err := fileWriter.ExtractCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(mp), "should be equal")
		assert.Equal(t, 3, len(mp["table1"]), "should be equal")
		assert.Equal(t, 2, len(mp["table2"]), "should be equal")
		assert.Equal(t, StatusNotProcess, mp["table1"]["id1"].Status, "should be equal")
		assert.Equal(t, StatusInProcessing, mp["table1"]["id2"].Status, "should be equal")
		assert.Equal(t, StatusPrepareProcess, mp["table1"]["id3"].Status, "should be equal")
		assert.Equal(t, StatusDone, mp["table2"]["id1"].Status, "should be equal")
		assert.Equal(t, StatusWaitFather, mp["table2"]["id10"].Status, "should be equal")
	}
}
