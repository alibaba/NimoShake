package main

import (
	"fmt"
	"os"
	"time"
	"encoding/json"
	"flag"

	"nimo-shake/configure"
	"nimo-shake/common"
	"nimo-shake/run"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

type Exit struct{ Code int }

func main() {
	defer LOG.Close()

	var err error
	// argument options
	configuration := flag.String("conf", "", "configuration path")
	version := flag.Bool("version", false, "show version")
	flag.Parse()

	if *version {
		fmt.Println(utils.Version)
		return
	}

	if *configuration == "" {
		fmt.Println(utils.Version)
		flag.PrintDefaults()
		return
	}

	conf.Options.Version = utils.Version

	var file *os.File
	if file, err = os.Open(*configuration); err != nil {
		crash(fmt.Sprintf("Configure file open failed. %v", err), -1)
	}

	configure := nimo.NewConfigLoader(file)
	configure.SetDateFormat(utils.GolangSecurityTime)
	if err := configure.Load(&conf.Options); err != nil {
		crash(fmt.Sprintf("Configure file %s parse failed. %v", *configuration, err), -2)
	}

	utils.InitialLogger(conf.Options.LogFile, conf.Options.LogLevel, conf.Options.LogBuffer)

	// sanitize options
	if err := sanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	utils.Welcome()
	utils.StartTime = fmt.Sprintf("%v", time.Now().Format(utils.GolangSecurityTime))

	// write pid
	if err = utils.WritePidById(conf.Options.Id, "."); err != nil {
		crash(fmt.Sprintf("write pid failed. %v", err), -5)
	}

	// print configuration
	if opts, err := json.Marshal(conf.Options); err != nil {
		crash(fmt.Sprintf("marshal configuration failed[%v]", err), -6)
	} else {
		LOG.Info("%v configuration: %s", conf.Options.Id, string(opts))
	}

	run.Start()

	LOG.Info("sync complete!")
}

func sanitizeOptions() error {
	if len(conf.Options.Id) == 0 {
		return fmt.Errorf("id[%v] shouldn't be empty", conf.Options.Id)
	}

	if conf.Options.SyncMode != utils.SyncModeAll && conf.Options.SyncMode != utils.SyncModeFull {
		return fmt.Errorf("sync_mode[%v] illegal, should in {all, full}", conf.Options.SyncMode)
	}

	if conf.Options.SourceAccessKeyID == "" {
		return fmt.Errorf("source.access_key_id shouldn't be empty")
	}

	if conf.Options.SourceSecretAccessKey == "" {
		return fmt.Errorf("source.secret_access_key shouldn't be empty")
	}

	if conf.Options.FilterCollectionBlack != "" && conf.Options.FilterCollectionWhite != "" {
		return fmt.Errorf("filter.collection.white and filter.collection.black can't both be given")
	}

	if conf.Options.QpsFull <= 0 || conf.Options.QpsIncr <= 0 {
		return fmt.Errorf("qps should > 0")
	}

	if conf.Options.QpsFullBatchNum <= 0 {
		conf.Options.QpsFullBatchNum = 128
	}
	if conf.Options.QpsIncrBatchNum <= 0 {
		conf.Options.QpsIncrBatchNum = 128
	}

	if conf.Options.TargetType != utils.TargetTypeMongo && conf.Options.TargetType != utils.TargetTypeAliyunDynamoProxy {
		return fmt.Errorf("conf.Options.TargetType[%v] supports {mongodb, aliyun_dynamo_proxy} currently", conf.Options.TargetType)
	}

	if len(conf.Options.TargetAddress) == 0 {
		return fmt.Errorf("target.address[%v] illegal", conf.Options.TargetAddress)
	}

	if conf.Options.FullConcurrency > 4096 || conf.Options.FullConcurrency == 0 {
		return fmt.Errorf("full.concurrency[%v] should in (0, 4096]", conf.Options.FullConcurrency)
	}

	if conf.Options.FullDocumentConcurrency > 4096 || conf.Options.FullDocumentConcurrency == 0 {
		return fmt.Errorf("full.document.concurrency[%v] should in (0, 4096]", conf.Options.FullDocumentConcurrency)
	}

	if conf.Options.FullDocumentParser > 4096 || conf.Options.FullDocumentParser == 0 {
		return fmt.Errorf("full.document.parser[%v] should in (0, 4096]", conf.Options.FullDocumentParser)
	}

	// always enable
	conf.Options.FullEnableIndexPrimary = true

	if conf.Options.ConvertType != utils.ConvertTypeRaw && conf.Options.ConvertType != utils.ConvertTypeChange {
		return fmt.Errorf("convert.type[%v] illegal", conf.Options.ConvertType)
	}

	if conf.Options.IncreaseConcurrency == 0 {
		return fmt.Errorf("increase.concurrency should > 0")
	}

	if conf.Options.TargetMongoDBType != "" && conf.Options.TargetMongoDBType != utils.TargetMongoDBTypeReplica &&
		conf.Options.TargetMongoDBType != utils.TargetMongoDBTypeSharding {
		return fmt.Errorf("illegal target.mongodb.type[%v]", conf.Options.TargetMongoDBType)
	}

	if conf.Options.TargetType == utils.TargetTypeMongo && conf.Options.TargetMongoDBExist != "" &&
		conf.Options.TargetMongoDBExist != utils.TargetMongoDBExistRename &&
		conf.Options.TargetMongoDBExist != utils.TargetMongoDBExistDrop ||
		conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy && conf.Options.TargetMongoDBExist != "" &&
		conf.Options.TargetMongoDBExist != utils.TargetMongoDBExistDrop {
		return fmt.Errorf("illegal target.mongodb.exist[%v] when target.type=%v",
			conf.Options.TargetMongoDBExist, conf.Options.TargetType)
	}
	// set ConvertType
	if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
		conf.Options.ConvertType = utils.ConvertTypeSame
	}

	if conf.Options.CheckpointAddress == "" {
		conf.Options.CheckpointAddress = conf.Options.TargetAddress
	}
	if conf.Options.CheckpointDb == "" {
		conf.Options.CheckpointDb = fmt.Sprintf("%s-%s", conf.Options.Id, "checkpoint")
	}

	return nil
}

func crash(msg string, errCode int) {
	fmt.Println(msg)
	panic(Exit{errCode})
}
