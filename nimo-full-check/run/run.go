package run

import (
	shakeUtils "nimo-shake/common"
	"nimo-full-check/configure"
	"nimo-full-check/checker"

	LOG "github.com/vinllen/log4go"
)

func Start() {
	if err := shakeUtils.InitSession(conf.Opts.SourceAccessKeyID, conf.Opts.SourceSecretAccessKey,
			conf.Opts.SourceSessionToken, conf.Opts.SourceRegion, conf.Opts.SourceEndpointUrl,
			3, 5000); err != nil {
		LOG.Crashf("init global session failed[%v]", err)
	}

	// create dynamo session
	dynamoSession, err := shakeUtils.CreateDynamoSession("info")
	if err != nil {
		LOG.Crashf("create dynamodb session failed[%v]", err)
	}

	// check mongodb connection
	mongoClient, err := shakeUtils.NewMongoConn(conf.Opts.TargetAddress, shakeUtils.ConnectModePrimary, true)
	if err != nil {
		LOG.Crashf("connect mongodb[%v] failed[%v]", conf.Opts.TargetAddress, err)
	}

	c := checker.NewChecker(dynamoSession, mongoClient)
	if c == nil {
		LOG.Crashf("create checker failed")
	}

	if err := c.Run(); err != nil {
		LOG.Crashf("checker runs failed[%v]", err)
	}

	LOG.Info("checker finishes!")
}
