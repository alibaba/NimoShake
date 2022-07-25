package checker

import (
	"fmt"
	"sync"

	shakeUtils "nimo-shake/common"
	shakeFilter "nimo-shake/filter"
	"nimo-full-check/configure"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
)

type Checker struct {
	dynamoSession *dynamodb.DynamoDB
	mongoClient   *shakeUtils.MongoConn
}

func NewChecker(dynamoSession *dynamodb.DynamoDB, mongoClient *shakeUtils.MongoConn) *Checker {
	return &Checker{
		dynamoSession: dynamoSession,
		mongoClient:   mongoClient,
	}
}

func (c *Checker) Run() error {
	// fetch all tables
	LOG.Info("start fetching table list")
	rawTableList, err := shakeUtils.FetchTableList(c.dynamoSession)
	if err != nil {
		return fmt.Errorf("fetch table list failed[%v]", err)
	}
	LOG.Info("finish fetching table list: %v", rawTableList)

	tableList := shakeFilter.FilterList(rawTableList)
	LOG.Info("filter table list: %v", tableList)

	// check table exist
	if err := c.checkTableExist(tableList); err != nil {
		return fmt.Errorf("check table exist failed[%v]", err)
	}

	// reset parallel if needed
	parallel := conf.Opts.Parallel
	if parallel > len(tableList) {
		parallel = len(tableList)
	}

	execChan := make(chan string, len(tableList))
	for _, table := range tableList {
		execChan <- table
	}

	var wg sync.WaitGroup
	wg.Add(len(tableList))
	for i := 0; i < parallel; i++ {
		go func(id int) {
			for {
				table, ok := <-execChan
				if !ok {
					break
				}

				LOG.Info("documentChecker[%v] starts checking table[%v]", id, table)
				dc := NewDocumentChecker(id, table, c.dynamoSession)
				dc.Run()

				LOG.Info("documentChecker[%v] finishes checking table[%v]", id, table)
				wg.Done()
			}
		}(i)
	}
	wg.Wait()

	LOG.Info("all documentCheckers finish")
	return nil
}

func (c *Checker) checkTableExist(tableList []string) error {
	collections, err := c.mongoClient.Session.DB(conf.Opts.Id).CollectionNames()
	if err != nil {
		return fmt.Errorf("get target collection names error[%v]", err)
	}

	LOG.Info("all table: %v", collections)

	collectionsMp := shakeUtils.StringListToMap(collections)
	notExist := make([]string, 0)
	for _, table := range tableList {
		if _, ok := collectionsMp[table]; !ok {
			notExist = append(notExist, table)
		}
	}

	if len(notExist) != 0 {
		return fmt.Errorf("table not exist on the target side: %v", notExist)
	}
	return nil
}
