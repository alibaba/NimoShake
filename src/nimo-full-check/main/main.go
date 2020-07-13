package main

import (
	"fmt"
	"os"
	"time"

	"nimo-full-check/run"
	"nimo-full-check/configure"
	"nimo-full-check/common"
	shakeUtils "nimo-shake/common"
	shakeFilter "nimo-shake/filter"

	"github.com/jessevdk/go-flags"
	LOG "github.com/vinllen/log4go"
)

type Exit struct{ Code int }

func main() {
	defer LOG.Close()

	// parse conf.Opts
	args, err := flags.Parse(&conf.Opts)

	if conf.Opts.Version {
		fmt.Println(utils.Version)
		os.Exit(0)
	}

	// 若err != nil, 会自动打印错误到 stderr
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "flag err %s\n", flagsErr)
			os.Exit(1)
		}
	}

	if len(args) != 0 {
		fmt.Fprintf(os.Stderr, "unexpected args %+v", args)
		os.Exit(1)
	}

	shakeUtils.InitialLogger("", conf.Opts.LogLevel, false)

	if err := sanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	LOG.Info("full-check starts")
	run.Start()
	LOG.Info("full-check completes!")
}

func sanitizeOptions() error {
	if len(conf.Opts.SourceAccessKeyID) == 0 || len(conf.Opts.SourceSecretAccessKey) == 0 {
		return fmt.Errorf("sourceAccessKeyID and sourceSecretAccessKey can't be empty")
	}

	if len(conf.Opts.TargetAddress) == 0 {
		return fmt.Errorf("targetAddress can't be empty")
	}

	if conf.Opts.Parallel <= 0 {
		return fmt.Errorf("parallel should >= 1, default is 16")
	}

	if conf.Opts.QpsFull == 0 {
		conf.Opts.QpsFull = 10000
	} else if conf.Opts.QpsFull < 0 {
		return fmt.Errorf("qps.full should > 0, default is 10000")
	}

	if conf.Opts.QpsFullBatchNum <= 0 {
		conf.Opts.QpsFullBatchNum = 128
	}

	shakeFilter.Init(conf.Opts.FilterCollectionWhite, conf.Opts.FilterCollectionBlack)

	if len(conf.Opts.DiffOutputFile) == 0 {
		return fmt.Errorf("diff output file shouldn't be empty")
	} else {
		_, err := os.Stat(conf.Opts.DiffOutputFile)
		if os.IsNotExist(err) {
			if err = os.Mkdir(conf.Opts.DiffOutputFile, os.ModePerm); err != nil {
				return fmt.Errorf("mkdir diffOutputFile[%v] failed[%v]", conf.Opts.DiffOutputFile, err)
			}
		} else {
			newName := fmt.Sprintf("%v-%v", conf.Opts.DiffOutputFile, time.Now().Format(shakeUtils.GolangSecurityTime))
			if err := os.Rename(conf.Opts.DiffOutputFile, newName); err != nil {
				return fmt.Errorf("diffOutputFile dir[%v] rename to newFile[%v] failed[%v], need to be remvoed manullay",
					conf.Opts.DiffOutputFile, newName, err)
			}
			if err = os.Mkdir(conf.Opts.DiffOutputFile, os.ModePerm); err != nil {
				return fmt.Errorf("mkdir diffOutputFile[%v] again failed[%v]", conf.Opts.DiffOutputFile, err)
			}
		}
	}

	if conf.Opts.ConvertType == "" {
		conf.Opts.ConvertType = shakeUtils.ConvertTypeChange
	} else if conf.Opts.ConvertType != shakeUtils.ConvertTypeRaw && conf.Opts.ConvertType != shakeUtils.ConvertTypeChange {
		return fmt.Errorf("convertType[%v] illegal", conf.Opts.ConvertType)
	}

	return nil
}

func crash(msg string, errCode int) {
	fmt.Println(msg)
	panic(Exit{errCode})
}
