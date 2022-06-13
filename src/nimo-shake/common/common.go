package utils

import (
	"fmt"
	"os"
	"strings"

	LOG "github.com/vinllen/log4go"
)

const (
	GolangSecurityTime = "2006-01-02T15:04:05Z"

	ConvertTypeRaw     = "raw"
	ConvertTypeChange  = "change"
	ConvertMTypeChange = "mchange"
	ConvertTypeSame    = "same" // used in dynamodb -> dynamo-proxy

	SyncModeAll  = "all"
	SyncModeFull = "full"
	SyncModeIncr = "incr"

	TargetTypeMongo             = "mongodb"
	TargetTypeAliyunDynamoProxy = "aliyun_dynamo_proxy"

	TargetMongoDBTypeReplica  = "replica"
	TargetMongoDBTypeSharding = "sharding"

	TargetDBExistRename = "rename"
	TargetDBExistDrop   = "drop"

	SIGNALPROFILE = 31
	SIGNALSTACK   = 30
)

var (
	Version   = "$"
	StartTime string
)

func InitialLogger(logFile string, level string, logBuffer bool) bool {
	logLevel := parseLogLevel(level)
	if len(logFile) != 0 {
		// create logs folder for log4go. because of its mistake that doesn't create !
		if err := os.MkdirAll("logs", os.ModeDir|os.ModePerm); err != nil {
			return false
		}
		if logBuffer {
			LOG.LogBufferLength = 32
		} else {
			LOG.LogBufferLength = 0
		}
		fileLogger := LOG.NewFileLogWriter(fmt.Sprintf("logs/%s", logFile), true)
		//fileLogger.SetRotateDaily(true)
		fileLogger.SetRotateSize(500 * 1024 * 1024)
		// fileLogger.SetFormat("[%D %T] [%L] [%s] %M")
		fileLogger.SetFormat("[%D %T] [%L] %M")
		fileLogger.SetRotateMaxBackup(100)
		LOG.AddFilter("file", logLevel, fileLogger)
	} else {
		LOG.AddFilter("console", logLevel, LOG.NewConsoleLogWriter())
	}
	return true
}

func parseLogLevel(level string) LOG.Level {
	switch strings.ToLower(level) {
	case "debug":
		return LOG.DEBUG
	case "info":
		return LOG.INFO
	case "warning":
		return LOG.WARNING
	case "error":
		return LOG.ERROR
	default:
		return LOG.DEBUG
	}
}

/**
 * block password in mongo_urls:
 * two kind mongo_urls:
 * 1. mongodb://username:password@address
 * 2. username:password@address
 */
func BlockMongoUrlPassword(url, replace string) string {
	colon := strings.Index(url, ":")
	if colon == -1 || colon == len(url)-1 {
		return url
	} else if url[colon+1] == '/' {
		// find the second '/'
		for colon++; colon < len(url); colon++ {
			if url[colon] == ':' {
				break
			}
		}

		if colon == len(url) {
			return url
		}
	}

	at := strings.Index(url, "@")
	if at == -1 || at == len(url)-1 || at <= colon {
		return url
	}

	newUrl := make([]byte, 0, len(url))
	for i := 0; i < len(url); i++ {
		if i <= colon || i > at {
			newUrl = append(newUrl, byte(url[i]))
		} else if i == at {
			newUrl = append(newUrl, []byte(replace)...)
			newUrl = append(newUrl, byte(url[i]))
		}
	}
	return string(newUrl)
}
