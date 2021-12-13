package protocal

import (
	"fmt"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	conf "nimo-shake/configure"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
)

type RawConverter struct {
}

// use dfs to convert to bson.M
func (rc *RawConverter) Run(input map[string]*dynamodb.AttributeValue) (interface{}, error) {
	v := reflect.ValueOf(input)
	if output := rc.dfs(v); output == nil {
		return RawData{}, fmt.Errorf("parse failed, return nil")
	} else if out, ok := output.(RawData); !ok {
		return RawData{}, fmt.Errorf("parse failed, return type isn't RawData")
	} else if _, ok := out.Data.(bson2.M); !ok {
		return RawData{}, fmt.Errorf("parse failed, return data isn't bson.M")
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
			md := out.(RawData)
			size += md.Size
			ret = append(ret, md.Data)
		}
		return RawData{size, ret}
	case reflect.Struct:
		if v.NumField() == 0 {
			return nil
		}

		size := 0
		ret := make(bson2.M)
		for i := 0; i < v.NumField(); i++ {
			name := v.Type().Field(i).Name
			if out := rc.dfs(v.Field(i)); out != nil {
				md := out.(RawData)
				size += md.Size
				size += len(name)
				if _, ok := md.Data.([]interface{}); ok {
					// is type array
					md.Data = rc.convertListToDetailList(name, md.Data)
				}
				ret[name] = md.Data
			}
		}
		return RawData{size, ret}
	case reflect.Map:
		if len(v.MapKeys()) == 0 {
			return nil
		}

		size := 0
		ret := make(bson2.M)
		for _, key := range v.MapKeys() {
			name := key.String()
			name = conf.ConvertIdFunc(name)
			if out := rc.dfs(v.MapIndex(key)); out != nil {
				md := out.(RawData)
				size += md.Size
				size += len(name)
				if _, ok := md.Data.([]interface{}); ok {
					// is type array
					out = rc.convertListToDetailList(name, md.Data)
				}
				ret[name] = md.Data
			}
		}
		return RawData{size, ret}
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
		return RawData{len(out), out}
	case reflect.Int:
		fallthrough
	case reflect.Int64:
		return RawData{8, v.Int()}
	case reflect.Int8:
		return RawData{1, int8(v.Int())}
	case reflect.Int16:
		return RawData{2, int16(v.Int())}
	case reflect.Int32:
		return RawData{4, int32(v.Int())}
	case reflect.Uint:
		fallthrough
	case reflect.Uint64:
		return RawData{8, v.Uint()}
	case reflect.Uint8:
		return RawData{1, uint8(v.Uint())}
	case reflect.Uint16:
		return RawData{2, uint16(v.Uint())}
	case reflect.Uint32:
		return RawData{4, uint32(v.Uint())}
	case reflect.Bool:
		// fake size
		return RawData{1, v.Bool()}
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
			output = append(output, ele.(bson2.M))
		}
		return output
	}
	return list
}