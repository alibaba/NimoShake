package run

import (
	"nimo-shake/full-sync"
	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/incr-sync"
	"nimo-shake/checkpoint"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"nimo-shake/filter"
)

func Start() {
	LOG.Info("check connections")

	// init filter
	filter.Init(conf.Options.FilterCollectionWhite, conf.Options.FilterCollectionBlack)

	if err := utils.InitSession(conf.Options.SourceAccessKeyID, conf.Options.SourceSecretAccessKey,
			conf.Options.SourceSessionToken, conf.Options.SourceRegion, conf.Options.SourceSessionMaxRetries,
			conf.Options.SourceSessionTimeout); err != nil {
		LOG.Crashf("init global session failed[%v]", err)
	}

	// check mongodb connection
	_, err := utils.NewMongoConn(conf.Options.TargetAddress, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Crashf("connect mongodb[%v] failed[%v]", conf.Options.TargetAddress, err)
	}

	// check checkpoint connection
	ckptConn, err := utils.NewMongoConn(conf.Options.CheckpointAddress, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Crashf("connect checkpoint mongodb[%v] failed[%v]", conf.Options.CheckpointAddress, err)
	}

	// create dynamo session
	dynamoSession, err := utils.CreateDynamoSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Crashf("create dynamodb session failed[%v]", err)
	}

	// create dynamo stream client
	dynamoStreamSession, err := utils.CreateDynamoStreamSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Crashf("create dynamodb stream session failed[%v]", err)
	}

	LOG.Info("check checkpoint")
	var skipFull bool
	var streamMap map[string]*dynamodbstreams.Stream
	if conf.Options.SyncMode == utils.SyncModeAll {
		skipFull, streamMap, err = checkpoint.CheckCkpt(conf.Options.CheckpointAddress, conf.Options.CheckpointDb, dynamoStreamSession)
		if err != nil {
			LOG.Crashf("check checkpoint failed[%v]", err)
		}
	}

	// full sync
	if skipFull == false {
		if conf.Options.SyncMode == utils.SyncModeAll {
			LOG.Info("drop old checkpoint")
			if err := checkpoint.DropCheckpoint(conf.Options.CheckpointAddress, conf.Options.CheckpointDb); err != nil &&
				err.Error() != utils.NotFountErr {
				LOG.Crashf("drop checkpoint failed[%v]", err)
			}

			LOG.Info("prepare checkpoint")
			streamMap, err = checkpoint.PrepareFullSyncCkpt(conf.Options.CheckpointAddress, conf.Options.CheckpointDb, dynamoSession,
				dynamoStreamSession)
			if err != nil {
				LOG.Crashf("prepare checkpoint failed[%v]", err)
			}

			// select{}
		} else {
			LOG.Info("sync.mode is 'full', no need to check checkpoint")
		}

		LOG.Info("start full sync")
		full_sync.Start(dynamoSession)
		LOG.Info("------------------------full sync done!------------------------")
	}

	if conf.Options.SyncMode == utils.SyncModeFull {
		LOG.Info("sync.mode is 'full', finish")
		return
	}

	// update checkpoint
	if err := checkpoint.UpsertStatus(ckptConn, conf.Options.CheckpointDb, checkpoint.CheckpointStatusTable, checkpoint.CheckpointStatusValueIncrSync); err != nil {
		LOG.Crashf("set checkpoint to [%v] failed[%v]", checkpoint.CheckpointStatusValueIncrSync, err)
	}
	LOG.Info("start increase sync")

	incr_sync.Start(streamMap)
}
