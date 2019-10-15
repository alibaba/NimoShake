package full_sync

import (
	"sync"
	"time"
	"fmt"

	"nimo-shake/common"
	"nimo-shake/configure"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/vinllen/mgo/bson"
	"nimo-shake/filter"
)

func Start(dynamoSession *dynamodb.DynamoDB) {
	// fetch all tables
	LOG.Info("start fetching table list")
	tableList, err := utils.FetchTableList(dynamoSession)
	if err != nil {
		LOG.Crashf("fetch table list failed[%v]", err)
	}
	LOG.Info("finish fetching table list: %v", tableList)

	tableList = filter.FilterList(tableList)

	if err := checkTableExists(tableList); err != nil {
		LOG.Crashf("check table exists failed[%v]", err)
		return
	}

	LOG.Info("start syncing: %v", tableList)

	fullChan := make(chan string, len(tableList))
	for _, table := range tableList {
		fullChan <- table
	}

	var wg sync.WaitGroup
	wg.Add(len(tableList))
	for i := 0; i < int(conf.Options.FullConcurrency); i++ {
		go func(id int) {
			for {
				table, ok := <-fullChan
				if !ok {
					// chan closed
					break
				}

				ts := NewTableSyncer(id, table)
				if ts == nil {
					LOG.Crashf("tableSyncer[%v] create failed", id)
				}

				LOG.Info("tableSyncer[%v] starts sync table[%v]", id, table)
				ts.Sync()
				LOG.Info("tableSyncer[%v] finish sync table[%v]", id, table)
				ts.Close()

				wg.Done()
			}
		}(i)
	}

	wg.Wait()
	close(fullChan)

	LOG.Info("finish syncing all tables and indexes!")
}

func checkTableExists(tableList []string) error {
	if conf.Options.TargetType == utils.TargetTypeMongo {
		LOG.Info("target.mongodb.exist is set[%v]", conf.Options.TargetMongoDBExist)

		mongoClient, err := utils.NewMongoConn(conf.Options.TargetAddress, utils.ConnectModePrimary, true)
		if err != nil {
			return fmt.Errorf("create mongodb session error[%v]", err)
		}

		now := time.Now().Format(utils.GolangSecurityTime)
		collections, err := mongoClient.Session.DB(conf.Options.Id).CollectionNames()
		if err != nil {
			return fmt.Errorf("get target collection names error[%v]", err)
		}

		collectionsMp := utils.StringListToMap(collections)
		for _, table := range tableList {
			// check exist on the target mongodb
			if _, ok := collectionsMp[table]; ok {
				// exist
				LOG.Info("table[%v] exists", table)
				if conf.Options.TargetMongoDBExist == utils.TargetMongoDBExistDrop {
					if err := mongoClient.Session.DB(conf.Options.Id).C(table).DropCollection(); err != nil {
						return fmt.Errorf("drop target collection[%v] failed[%v]", table, err)
					}
				} else if conf.Options.TargetMongoDBExist == utils.TargetMongoDBExistRename {
					fromCollection := fmt.Sprintf("%s.%s", conf.Options.Id, table)
					toCollection := fmt.Sprintf("%s.%s_%v", conf.Options.Id, table, now)
					if err := mongoClient.Session.DB("admin").Run(bson.D{
						bson.DocElem{"renameCollection", fromCollection},
						bson.DocElem{"to", toCollection},
						bson.DocElem{"dropTarget", false},
					}, nil); err != nil {
						return fmt.Errorf("rename target collection[%v] failed[%v]", table, err)
					}
				} else {
					return fmt.Errorf("collection[%v] exists on the target", table)
				}
			}
		}
		LOG.Info("finish handling table exists")
	}

	return nil
}
