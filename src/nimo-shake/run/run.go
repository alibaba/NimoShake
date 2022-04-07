package run

import (
	"nimo-shake/checkpoint"
	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/filter"
	"nimo-shake/full-sync"
	"nimo-shake/incr-sync"
	"nimo-shake/writer"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

func incrStart(streamMap map[string]*dynamodbstreams.Stream, ckptWriter checkpoint.Writer) {
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

	LOG.Info("------------------------start incr sync------------------------")
	incr_sync.Start(streamMap, ckptWriter)
	LOG.Info("------------------------end incr sync------------------------")
}

func Start() {
	LOG.Info("check connections")

	utils.FullSyncInitHttpApi(conf.Options.FullSyncHTTPListenPort)
	utils.IncrSyncInitHttpApi(conf.Options.IncrSyncHTTPListenPort)

	// init filter
	filter.Init(conf.Options.FilterCollectionWhite, conf.Options.FilterCollectionBlack)

	if err := utils.InitSession(conf.Options.SourceAccessKeyID, conf.Options.SourceSecretAccessKey,
		conf.Options.SourceSessionToken, conf.Options.SourceRegion, conf.Options.SourceEndpointUrl,
		conf.Options.SourceSessionMaxRetries, conf.Options.SourceSessionTimeout); err != nil {
		LOG.Crashf("init global session failed[%v]", err)
	}

	// check writer connection
	w := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress,
		utils.NS{"nimo-shake", "shake_writer_test"}, conf.Options.LogLevel)
	if w == nil {
		LOG.Crashf("connect type[%v] address[%v] failed[%v]",
			conf.Options.TargetType, conf.Options.TargetAddress)
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

	var skipFull bool
	var streamMap map[string]*dynamodbstreams.Stream
	if conf.Options.SyncMode == utils.SyncModeAll {
		LOG.Info("------------------------check checkpoint------------------------")
		skipFull, streamMap, err = checkpoint.CheckCkpt(ckptWriter, dynamoStreamSession)
		if err != nil {
			LOG.Crashf("check checkpoint failed[%v]", err)
		}
		LOG.Info("------------------------end check checkpoint------------------------")
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
			LOG.Info("------------------------drop old checkpoint------------------------")
			if err := ckptWriter.DropAll(); err != nil && err.Error() != utils.NotFountErr {
				LOG.Crashf("drop checkpoint failed[%v]", err)
			}

			LOG.Info("------------------------prepare checkpoint start------------------------")
			streamMap, err = checkpoint.PrepareFullSyncCkpt(ckptWriter, dynamoSession, dynamoStreamSession)
			if err != nil {
				LOG.Crashf("prepare checkpoint failed[%v]", err)
			}
			LOG.Info("------------------------prepare checkpoint done------------------------")

			// select{}
		} else {
			LOG.Info("sync.mode is 'full', no need to check checkpoint")
		}

		if conf.Options.IncrSyncParallel == true {
			go incrStart(streamMap, ckptWriter)
		}

		// update checkpoint
		if err := ckptWriter.UpdateStatus(checkpoint.CheckpointStatusValueFullSync); err != nil {
			LOG.Crashf("set checkpoint to [%v] failed[%v]", checkpoint.CheckpointStatusValueFullSync, err)
		}

		LOG.Info("------------------------start full sync------------------------")
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

	if conf.Options.IncrSyncParallel == false {
		go incrStart(streamMap, ckptWriter)
	}

	select {}
}
