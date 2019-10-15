package checkpoint

import (
	"fmt"
	"sync"

	"nimo-shake/common"

	"github.com/vinllen/mgo/bson"
)

var (
	GlobalShardIteratorMap = ShardIteratorMap{
		mp: make(map[string]string),
	}
)

type ShardIteratorMap struct {
	mp   map[string]string
	lock sync.Mutex
}

func (sim *ShardIteratorMap) Set(key, iterator string) bool {
	sim.lock.Lock()
	defer sim.lock.Unlock()

	if _, ok := sim.mp[key]; ok {
		return false
	}

	sim.mp[key] = iterator
	return false
}

func (sim *ShardIteratorMap) Get(key string) (string, bool) {
	sim.lock.Lock()
	defer sim.lock.Unlock()

	it, ok := sim.mp[key]
	return it, ok
}

func (sim *ShardIteratorMap) Delete(key string) bool {
	sim.lock.Lock()
	defer sim.lock.Unlock()

	if _, ok := sim.mp[key]; ok {
		delete(sim.mp, key)
		return true
	}
	return false
}

func ExtractCheckpoint(ckptClient *utils.MongoConn, ckptDb string) (map[string]map[string]*Checkpoint, error) {
	// extract checkpoint from mongodb
	ckptMap := make(map[string]map[string]*Checkpoint)

	collectionList, err := ckptClient.Session.DB(ckptDb).CollectionNames()
	if err != nil {
		return nil, fmt.Errorf("fetch checkpoint collection list failed[%v]", err)
	}
	for _, table := range collectionList {
		if FilterCkptCollection(table) {
			continue
		}

		innerMap, err := ExtractSingleCheckpoint(ckptClient, ckptDb, table)
		if err != nil {
			return nil, err
		}
		ckptMap[table] = innerMap
	}

	return ckptMap, nil
}

func ExtractSingleCheckpoint(ckptClient *utils.MongoConn, ckptDb, table string) (map[string]*Checkpoint, error) {
	innerMap := make(map[string]*Checkpoint)
	/*data := make([]byte, 0)
	it := ckptClient.Session.DB(ckptDb).C(table).Find(bson.M{}).Iter()
	for it.Next(&data) {
		fmt.Println("aaa ")
		if err := it.Err(); err != nil {
			return nil, err
		}

		var ckpt Checkpoint
		if err := bson.Unmarshal(data, &ckpt); err != nil {
			return nil, fmt.Errorf("unmarsal data[%v] in collection[%v] failed[%v]", data, table, err)
		}
		innerMap[ckpt.ShardId] = &ckpt
	}*/

	data := make([]*Checkpoint, 0)
	err := ckptClient.Session.DB(ckptDb).C(table).Find(bson.M{}).All(&data)
	if err != nil {
		return nil, err
	}

	for _, ele := range data {
		innerMap[ele.ShardId] = ele
	}

	return innerMap, nil
}


func InsertCkpt(ckpt *Checkpoint, targetClient *utils.MongoConn, db, collection string) error {
	return targetClient.Session.DB(db).C(collection).Insert(*ckpt)
}

func UpdateCkpt(shardId string, ckpt *Checkpoint, targetClient *utils.MongoConn, db, collection string) error {
	return targetClient.Session.DB(db).C(collection).Update(bson.M{"shard_id": shardId}, *ckpt)
}

func UpdateCkptSet(shardId string, input bson.M, targetClient *utils.MongoConn, db, collection string) error {
	return targetClient.Session.DB(db).C(collection).Update(bson.M{"shard_id": shardId}, bson.M{"$set": input})
}

func QueryCkpt(shardId string, targetClient *utils.MongoConn, db, collection string) (Checkpoint, error) {
	var res Checkpoint
	err := targetClient.Session.DB(db).C(collection).Find(bson.M{"shard_id": shardId}).One(&res)
	return res, err
}

func DropCheckpoint(address, db string) error {
	targetClient, err := utils.NewMongoConn(address, utils.ConnectModePrimary, true)
	if err != nil {
		return fmt.Errorf("connect checkpoint database[%v] failed[%v]", address, err)
	}

	return targetClient.Session.DB(db).DropDatabase()
}
