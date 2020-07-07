package utils

import (
	"github.com/vinllen/mgo"
)

// true means error can be ignored
// https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
func MongodbIgnoreError(err error, op string, isFullSyncStage bool) bool {
	errorCode := mgo.ErrorCodeList(err)
	if err != nil && len(errorCode) == 0 {
		return false
	}

	for _, err := range errorCode {
		switch op {
		case "i":
		case "u":
			if isFullSyncStage {
				if err == 28 { // PathNotViable
					continue
				}
			}
		case "d":
			if err == 26 { // NamespaceNotFound
				continue
			}
		case "c":
			if err == 26 { // NamespaceNotFound
				continue
			}
		default:
			return false
		}
		return false
	}

	return true
}

func DynamoIgnoreError(err error, op string, isFullSyncStage bool) bool {
	switch op {
	case "i":
	case "u":
		if isFullSyncStage {
			if err.Error() == "xxxx" { // PathNotViable
				return true
			}
		}
	case "d":
		if err.Error() == "xxxx" { // NamespaceNotFound
			return true
		}
	case "c":
		if err.Error() == "xxxx" { // NamespaceNotFound
			return true
		}
	default:
		return false
	}
	return false
}