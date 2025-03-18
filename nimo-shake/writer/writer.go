package writer

import (
	utils "nimo-shake/common"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
)

type Writer interface {
	// create table
	CreateTable(tableDescribe *dynamodb.TableDescription) error
	// pass table description
	PassTableDesc(tableDescribe *dynamodb.TableDescription)
	// drop table
	DropTable() error
	// write bulk data, used in full sync
	WriteBulk(input []interface{}) error
	// insert
	Insert(input []interface{}, index []interface{}) error
	// delete
	Delete(input []interface{}) error
	// update
	Update(input []interface{}, index []interface{}) error
	// close
	Close()
	// get session
	GetSession() interface{}
}

func NewWriter(name, address string, ns utils.NS, logLevel string) Writer {
	var w Writer = nil
	switch name {
	case utils.TargetTypeMongo:
		// return NewMongoWriter(name, address, ns)
		w = NewMongoCommunityWriter(name, address, ns)
	case utils.TargetTypeAliyunDynamoProxy:
		w = NewDynamoProxyWriter(name, address, ns, logLevel)
	default:
		LOG.Crashf("unknown writer[%v]", name)
	}

	if IsNil(w) {
		LOG.Crashf("create writer[%v] failed", name)
	}

	return w
}

func IsNil(w Writer) bool {
	if w == nil {
		return true
	}

	v := reflect.ValueOf(w)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
