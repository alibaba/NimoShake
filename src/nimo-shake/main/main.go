package main

import (
	"encoding/json"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"
	"syscall"
	"time"

	"nimo-shake/checkpoint"
	"nimo-shake/common"
	"nimo-shake/configure"
	"nimo-shake/run"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

type Exit struct{ Code int }

func main() {
	defer LOG.Close()

	//http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
	//go func() {
	//	http.ListenAndServe(":6060", nil)
	//}()

	runtime.GOMAXPROCS(256)
	fmt.Println("max process:", runtime.GOMAXPROCS(0))

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

	// read fcv and do comparison
	if _, err := conf.CheckFcv(*configuration, utils.FcvConfiguration.FeatureCompatibleVersion); err != nil {
		crash(err.Error(), -5)
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

	nimo.Profiling(int(conf.Options.SystemProfile))
	nimo.RegisterSignalForProfiling(syscall.Signal(utils.SIGNALPROFILE)) // syscall.SIGUSR2
	nimo.RegisterSignalForPrintStack(syscall.Signal(utils.SIGNALSTACK), func(bytes []byte) { // syscall.SIGUSR1
		LOG.Info(string(bytes))
	})

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

	if conf.Options.IncrSyncParallel != true {
		conf.Options.IncrSyncParallel = false
	} else {
		if conf.Options.SyncMode != utils.SyncModeAll {
			return fmt.Errorf("sync_mode must be all when incr_sync_parallel is true")
		}
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

	if conf.Options.FullDocumentWriteBatch <= 0 {
		if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
			conf.Options.FullDocumentWriteBatch = 25
		} else {
			conf.Options.FullDocumentWriteBatch = 128
		}
	} else if conf.Options.FullDocumentWriteBatch > 25 && conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
		conf.Options.FullDocumentWriteBatch = 25
	}

	if conf.Options.FullReadConcurrency <= 0 {
		conf.Options.FullReadConcurrency = 1
	} else if conf.Options.FullReadConcurrency > 8192 {
		return fmt.Errorf("full.read.concurrency[%v] should in (0, 8192]", conf.Options.FullReadConcurrency)
	}

	if conf.Options.FullDocumentParser > 4096 || conf.Options.FullDocumentParser == 0 {
		return fmt.Errorf("full.document.parser[%v] should in (0, 4096]", conf.Options.FullDocumentParser)
	}

	// always enable
	conf.Options.FullEnableIndexPrimary = true

	if conf.Options.ConvertType == "" {
		conf.Options.ConvertType = utils.ConvertMTypeChange
	}
	if conf.Options.ConvertType != utils.ConvertTypeRaw &&
		conf.Options.ConvertType != utils.ConvertTypeChange &&
		conf.Options.ConvertType != utils.ConvertMTypeChange {
		return fmt.Errorf("convert.type[%v] illegal", conf.Options.ConvertType)
	}

	if conf.Options.IncreaseConcurrency == 0 {
		return fmt.Errorf("increase.concurrency should > 0")
	}

	if conf.Options.TargetMongoDBType != "" && conf.Options.TargetMongoDBType != utils.TargetMongoDBTypeReplica &&
		conf.Options.TargetMongoDBType != utils.TargetMongoDBTypeSharding {
		return fmt.Errorf("illegal target.mongodb.type[%v]", conf.Options.TargetMongoDBType)
	}

	if conf.Options.TargetType == utils.TargetTypeMongo && conf.Options.TargetDBExist != "" &&
		conf.Options.TargetDBExist != utils.TargetDBExistRename &&
		conf.Options.TargetDBExist != utils.TargetDBExistDrop ||
		conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy && conf.Options.TargetDBExist != "" &&
			conf.Options.TargetDBExist != utils.TargetDBExistDrop {
		return fmt.Errorf("target.mongodb.exist[%v] should be 'drop' when target.type=%v",
			conf.Options.TargetDBExist, conf.Options.TargetType)
	}
	// set ConvertType
	if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
		conf.Options.ConvertType = utils.ConvertTypeSame
	}

	// checkpoint
	if conf.Options.CheckpointType == "" {
		conf.Options.CheckpointType = checkpoint.CheckpointWriterTypeFile
	}
	if conf.Options.CheckpointType == checkpoint.CheckpointWriterTypeMongo &&
		conf.Options.CheckpointAddress == "" &&
		conf.Options.TargetType != utils.TargetTypeMongo {
		return fmt.Errorf("checkpoint.type should == file when checkpoint.address is empty and target.type != mongodb")
	}

	if conf.Options.CheckpointAddress == "" {
		if conf.Options.TargetType == utils.TargetTypeMongo {
			conf.Options.CheckpointAddress = conf.Options.TargetAddress
		} else {
			conf.Options.CheckpointAddress = "checkpoint"
		}
	}
	if conf.Options.CheckpointDb == "" {
		conf.Options.CheckpointDb = fmt.Sprintf("%s-%s", conf.Options.Id, "checkpoint")
	}

	if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy &&
		(!conf.Options.IncreaseExecutorUpsert || !conf.Options.IncreaseExecutorInsertOnDupUpdate) {
		return fmt.Errorf("increase.executor.upsert and increase.executor.insert_on_dup_update should be "+
			"enable when target type is %v", utils.TargetTypeAliyunDynamoProxy)
	}

	if conf.Options.SourceEndpointUrl != "" && conf.Options.SyncMode != "full" {
		return fmt.Errorf("only support sync.mode=full when source.endpoint_url is set")
	}

	return nil
}

func crash(msg string, errCode int) {
	fmt.Println(msg)
	panic(Exit{errCode})
}
