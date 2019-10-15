package protocal

import (
	"reflect"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
)

type RawConverter struct {
}

// use dfs to convert to bson.M
func (rc *RawConverter) Run(input map[string]*dynamodb.AttributeValue) (MongoData, error) {
	v := reflect.ValueOf(input)
	if output := rc.dfs(v); output == nil {
		return MongoData{}, fmt.Errorf("parse failed, return nil")
	} else if out, ok := output.(MongoData); !ok {
		return MongoData{}, fmt.Errorf("parse failed, return type isn't MongoData")
	} else if _, ok := out.Data.(bson.M); !ok {
		return MongoData{}, fmt.Errorf("parse failed, return data isn't bson.M")
	} else {
		return out, nil
	}
}

func (rc *RawConverter) dfs(v reflect.Value) interface{} {
	switch v.Kind() {
	case reflect.Invalid:
		return nil
	case reflect.Slice, reflect.Array:
		if v.Len() == 0 {
			return nil
		}

		size := 0
		ret := make([]interface{}, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			out := rc.dfs(v.Index(i))
			md := out.(MongoData)
			size += md.Size
			ret = append(ret, md.Data)
		}
		return MongoData{size, ret}
	case reflect.Struct:
		if v.NumField() == 0 {
			return nil
		}

		size := 0
		ret := make(bson.M)
		for i := 0; i < v.NumField(); i++ {
			name := v.Type().Field(i).Name
			if out := rc.dfs(v.Field(i)); out != nil {
				md := out.(MongoData)
				size += md.Size
				size += len(name)
				if _, ok := md.Data.([]interface{}); ok {
					// is type array
					md.Data = rc.convertListToDetailList(name, md.Data)
				}
				ret[name] = md.Data
			}
		}
		return MongoData{size, ret}
	case reflect.Map:
		if len(v.MapKeys()) == 0 {
			return nil
		}

		size := 0
		ret := make(bson.M)
		for _, key := range v.MapKeys() {
			name := key.String()
			if out := rc.dfs(v.MapIndex(key)); out != nil {
				md := out.(MongoData)
				size += md.Size
				size += len(name)
				if _, ok := md.Data.([]interface{}); ok {
					// is type array
					out = rc.convertListToDetailList(name, md.Data)
				}
				ret[name] = md.Data
			}
		}
		return MongoData{size, ret}
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		} else {
			return rc.dfs(v.Elem())
		}
	case reflect.Interface:
		if v.IsNil() {
			return nil
		} else {
			return rc.dfs(v.Elem())
		}
	case reflect.String:
		out := v.String()
		return MongoData{len(out), out}
	case reflect.Int:
		fallthrough
	case reflect.Int64:
		return MongoData{8, v.Int()}
	case reflect.Int8:
		return MongoData{1, int8(v.Int())}
	case reflect.Int16:
		return MongoData{2, int16(v.Int())}
	case reflect.Int32:
		return MongoData{4, int32(v.Int())}
	case reflect.Uint:
		fallthrough
	case reflect.Uint64:
		return MongoData{8, v.Uint()}
	case reflect.Uint8:
		return MongoData{1, uint8(v.Uint())}
	case reflect.Uint16:
		return MongoData{2, uint16(v.Uint())}
	case reflect.Uint32:
		return MongoData{4, uint32(v.Uint())}
	case reflect.Bool:
		// fake size
		return MongoData{1, v.Bool()}
	default:
		// not support
		LOG.Error("unknown type[%v]", v.Kind())
		return nil
	}
}

func (rc *RawConverter) convertListToDetailList(name string, input interface{}) interface{} {
	list := input.([]interface{})
	switch name {
	case "B":
		output := make([]byte, 0, len(list))
		for _, ele := range list {
			output = append(output, ele.(byte))
		}
		return output
	case "BS":
		output := make([][]byte, 0, len(list))
		for _, ele := range list {
			inner := rc.convertListToDetailList("B", ele)
			output = append(output, inner.([]byte))
		}
		return output
	case "NS":
		fallthrough
	case "SS":
		output := make([]interface{}, 0, len(list))
		for _, ele := range list {
			output = append(output, ele.(string))
		}
		return output
	case "L":
		output := make([]interface{}, 0, len(list))
		for _, ele := range list {
			output = append(output, ele.(bson.M))
		}
		return output
	}
	return list
}