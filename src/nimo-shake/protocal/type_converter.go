package protocal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	LOG "github.com/vinllen/log4go"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	"github.com/vinllen/mongo-go-driver/bson/primitive"
	conf "nimo-shake/configure"
	"reflect"
	"strconv"
)

type TypeConverter struct {
}

// use dfs to convert to bson.M
func (tc *TypeConverter) Run(input map[string]*dynamodb.AttributeValue) (interface{}, error) {
	v := reflect.ValueOf(input)
	if output := tc.dfs(v); output == nil {
		return RawData{}, fmt.Errorf("parse failed, return nil")
	} else if out, ok := output.(RawData); !ok {
		return RawData{}, fmt.Errorf("parse failed, return type isn't RawData")
	} else if _, ok := out.Data.(bson2.M); !ok {
		return RawData{}, fmt.Errorf("parse failed, return data isn't bson.M")
	} else {
		return out, nil
	}
}

func (tc *TypeConverter) dfs(v reflect.Value) interface{} {
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
			out := tc.dfs(v.Index(i))
			md := out.(RawData)
			size += md.Size
			ret = append(ret, md.Data)
		}
		return RawData{size, ret}
	case reflect.Struct:
		if v.NumField() == 0 {
			return nil
		}
		if v.Type().Name() == "bson.Decimal128" {
			return RawData{16, v}
		}

		size := 0
		var ret interface{}
		cnt := 0
		// at most one field in AttributeValue
		for i := 0; i < v.NumField(); i++ {
			name := v.Type().Field(i).Name
			if out := tc.dfs(v.Field(i)); out != nil {
				cnt++
				if cnt > 2 {
					LOG.Crashf("illegal struct field number")
				}

				md := out.(RawData)
				size += md.Size
				md.Data = tc.convertToDetail(name, md.Data)
				ret = md.Data
			}
		}
		return RawData{size, ret}
	case reflect.Map:
		//TODO(zhangst) MapKeys()没有必要执行两次

		if len(v.MapKeys()) == 0 {
			return nil
		}

		size := 0
		ret := make(bson2.M)
		for _, key := range v.MapKeys() {
			name := key.String()
			name = conf.ConvertIdFunc(name)
			if out := tc.dfs(v.MapIndex(key)); out != nil {
				md := out.(RawData)
				size += md.Size
				size += len(name)
				// out = tc.convertToDetail(name, md.Data, false)
				ret[name] = md.Data
			}
		}
		return RawData{size, ret}
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		} else {
			return tc.dfs(v.Elem())
		}
	case reflect.Interface:
		if v.IsNil() {
			return nil
		} else {
			return tc.dfs(v.Elem())
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

func (tc *TypeConverter) convertToDetail(name string, input interface{}) interface{} {
	switch name {
	case "B":
		list := input.([]interface{})
		output := make([]byte, 0, len(list))
		for _, ele := range list {
			output = append(output, ele.(byte))
		}
		return output
	case "BS":
		list := input.([]interface{})
		output := make([][]byte, 0, len(list))
		for _, ele := range list {
			inner := tc.convertToDetail("B", ele)
			output = append(output, inner.([]byte))
		}
		return output
	case "NS":
		list := input.([]interface{})

		var nType reflect.Type
		for _, ele := range list {
			inner := tc.convertToDetail("N", ele)
			nType = reflect.TypeOf(inner)
			break
		}

		if nType.Name() == "int" {

			output := make([]int, 0, len(list))
			for _, ele := range list {
				inner := tc.convertToDetail("N", ele)
				output = append(output, inner.(int))
			}

			return output
		} else {
			output := make([]primitive.Decimal128, 0, len(list))
			for _, ele := range list {
				inner := tc.convertToDetail("N", ele)
				output = append(output, inner.(primitive.Decimal128))
			}

			return output
		}

	case "SS":
		list := input.([]interface{})
		output := make([]string, 0, len(list))
		for _, ele := range list {
			inner := tc.convertToDetail("S", ele)
			output = append(output, inner.(string))
		}
		return output
	case "L":
		list := input.([]interface{})
		output := make([]interface{}, 0, len(list))
		for _, ele := range list {
			output = append(output, ele)
		}
		return output
	case "BOOL":
		fallthrough
	case "NULL":
		return input.(bool)
	case "N":
		v := input.(string)
		// for new driver, we need to parse the value to mongo-go-driver.bson.decimal128, not mgo.bson.decimal128
		//val, err := bson.ParseDecimal128(v)
		//if err != nil {
		//	LOG.Error("convert N to decimal128 failed[%v]", err)
		//	val2, err := strconv.ParseFloat(v, 64)
		//	if err != nil {
		//		LOG.Crashf("convert N to decimal128 and float64 both failed[%v]", err)
		//	}
		//
		//	val, _ = bson.ParseDecimal128(fmt.Sprintf("%v", val2))
		//	return val
		//}

		val_int, err := strconv.Atoi(v)
		if err == nil {
			return val_int
		}

		val, err := primitive.ParseDecimal128(v)
		if err != nil {
			LOG.Error("convert N to decimal128 failed[%v]", err)
			val2, err := strconv.ParseFloat(v, 64)
			if err != nil {
				LOG.Crashf("convert N to decimal128 and float64 both failed[%v]", err)
			}

			val, _ = primitive.ParseDecimal128(fmt.Sprintf("%v", val2))
			return val
		}
		return val
	case "S":
		return input.(string)
	}

	// "M"
	return input
}