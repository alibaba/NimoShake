package utils

import (
	"os"
	"fmt"
	"strings"

	LOG "github.com/vinllen/log4go"
)

const (
	GolangSecurityTime = "2006-01-02T15:04:05Z"

	ConvertTypeRaw    = "raw"
	ConvertTypeChange = "change"

	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB

	SyncModeAll  = "all"
	SyncModeFull = "full"
	SyncModeIncr = "incr"

	TargetTypeMongo = "mongodb"

	TargetMongoDBTypeReplica  = "replica"
	TargetMongoDBTypeSharding = "sharding"

	TargetMongoDBExistRename = "rename"
	TargetMongoDBExistDrop   = "drop"
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
		fileLogger.SetRotateDaily(true)
		fileLogger.SetFormat("[%D %T] [%L] [%s] %M")
		fileLogger.SetRotateMaxBackup(7)
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
