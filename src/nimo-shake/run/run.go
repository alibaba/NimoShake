package run

import (
	"nimo-shake/full-sync"
	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/incr-sync"
	"nimo-shake/checkpoint"
	"nimo-shake/filter"
	"nimo-shake/writer"

	LOG "github.com/vinllen/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/gugemichael/nimo4go"
)

func Start() {
	LOG.Info("check connections")

	utils.FullSyncInitHttpApi(conf.Options.FullSyncHTTPListenPort)
	utils.IncrSyncInitHttpApi(conf.Options.IncrSyncHTTPListenPort)

	// init filter
	filter.Init(conf.Options.FilterCollectionWhite, conf.Options.FilterCollectionBlack)

	if err := utils.InitSession(conf.Options.SourceAccessKeyID, conf.Options.SourceSecretAccessKey,
			conf.Options.SourceSessionToken, conf.Options.SourceRegion, conf.Options.SourceSessionMaxRetries,
			conf.Options.SourceSessionTimeout); err != nil {
		LOG.Crashf("init global session failed[%v]", err)
	}

	// check writer connection
	w := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress, utils.NS{"nimo-shake", "shake_writer_test"}, conf.Options.LogLevel)
	if w == nil {
		LOG.Crashf("connect type[%v] address[%v] failed[%v]", conf.Options.TargetType, conf.Options.TargetAddress)
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

	LOG.Info("create checkpoint writer: type=%v", conf.Options.CheckpointType)
	ckptWriter := checkpoint.NewWriter(conf.Options.CheckpointType, conf.Options.CheckpointAddress,
		conf.Options.CheckpointDb)

	LOG.Info("check checkpoint")
	var skipFull bool
	var streamMap map[string]*dynamodbstreams.Stream
	if conf.Options.SyncMode == utils.SyncModeAll {
		skipFull, streamMap, err = checkpoint.CheckCkpt(ckptWriter, dynamoStreamSession)
		if err != nil {
			LOG.Crashf("check checkpoint failed[%v]", err)
		}
	}

	// full sync
	if skipFull == false {
		// register restful api
		full_sync.RestAPI()

		// start http server.
		nimo.GoRoutine(func() {
			// before starting, we must register all interface
			if err := utils.FullSyncHttpApi.Listen(); err != nil {
				LOG.Critical("start full sync server with port[%v] failed: %v", conf.Options.FullSyncHTTPListenPort,
					err)
			}
		})

		if conf.Options.SyncMode == utils.SyncModeAll {
			LOG.Info("drop old checkpoint")
			if err := ckptWriter.DropAll(); err != nil && err.Error() != utils.NotFountErr {
				LOG.Crashf("drop checkpoint failed[%v]", err)
			}

			LOG.Info("prepare checkpoint")
			streamMap, err = checkpoint.PrepareFullSyncCkpt(ckptWriter, dynamoSession, dynamoStreamSession)
			if err != nil {
				LOG.Crashf("prepare checkpoint failed[%v]", err)
			}

			// select{}
		} else {
			LOG.Info("sync.mode is 'full', no need to check checkpoint")
		}

		LOG.Info("start full sync")
		full_sync.Start(dynamoSession, w)
		LOG.Info("------------------------full sync done!------------------------")
	}

	if conf.Options.SyncMode == utils.SyncModeFull {
		LOG.Info("sync.mode is 'full', finish")
		return
	}

	if conf.Options.SyncSchemaOnly {
		LOG.Info("sync_schema_only enabled, finish")
		return
	}

	// update checkpoint
	if err := ckptWriter.UpdateStatus(checkpoint.CheckpointStatusValueIncrSync); err != nil {
		LOG.Crashf("set checkpoint to [%v] failed[%v]", checkpoint.CheckpointStatusValueIncrSync, err)
	}
	LOG.Info("start increase sync")

	// register restful api
	incr_sync.RestAPI()

	// start http server.
	nimo.GoRoutine(func() {
		// before starting, we must register all interface
		if err := utils.IncrSyncHttpApi.Listen(); err != nil {
			LOG.Critical("start incr sync server with port[%v] failed: %v", conf.Options.IncrSyncHTTPListenPort,
				err)
		}
	})
	incr_sync.Start(streamMap, ckptWriter)
}
