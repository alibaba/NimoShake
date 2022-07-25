package protocal

import (
	"testing"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"strconv"
)

func TestRawConverter(t *testing.T) {
	// test RawConverter

	var nr int
	{
		fmt.Printf("TestRawConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
		}

		rc := new(RawConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": bson.M{
				"N": "12345",
			},
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestRawConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
			"fuck": {
				S: aws.String("hello"),
			},
		}

		rc := new(RawConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": bson.M {
				"N": "12345",
			},
			"fuck": bson.M {
				"S": "hello",
			},
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestRawConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
			"fuck": {
				S: aws.String("hello"),
			},
			"test-string-list": {
				SS: []*string{aws.String("z1"), aws.String("z2"), aws.String("z3")},
			},
			"test-number-list": {
				NS: []*string{aws.String("123"), aws.String("456"), aws.String("78999999999999999999999999999")},
			},
			"test-bool": {
				BOOL: aws.Bool(true),
			},
			"test-byte": {
				B: []byte{123, 45, 78, 0, 12},
			},
			"test-byte-list": {
				BS: [][]byte{
					{123, 33, 44, 0, 55},
					{0, 1, 2, 0, 5},
				},
			},
		}

		rc := new(RawConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": bson.M {
				"N": "12345",
			},
			"fuck": bson.M {
				"S": "hello",
			},
			"test-string-list": bson.M {
				"SS": []interface{}{"z1", "z2", "z3"},
			},
			"test-number-list": bson.M {
				"NS": []interface{}{"123", "456", "78999999999999999999999999999"},
			},
			"test-bool": bson.M {
				"BOOL": true,
			},
			"test-byte": bson.M {
				"B": []byte{123, 45, 78, 0, 12},
			},
			"test-byte-list": bson.M{
				"BS": [][]byte{
					{123, 33, 44, 0, 55},
					{0, 1, 2, 0, 5},
				},
			},
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestRawConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
			"test-inner-struct": {
				L: []*dynamodb.AttributeValue {
					{
						S: aws.String("hello-inner"),
						N: aws.String("12345"),
					},
					{
						SS: []*string{aws.String("zi1"), aws.String("zi2"), aws.String("zi3")},
					},
				},
			},
			"test-inner-map": {
				M: map[string]*dynamodb.AttributeValue{
					"test": {
						N: aws.String("12345000"),
					},
				},
			},
			"test-NULL": {
				NULL: aws.Bool(false),
			},
		}

		rc := new(RawConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": bson.M {
				"N": "12345",
			},
			"test-inner-struct": bson.M {
				"L": []interface{} {
					bson.M{
						"S": "hello-inner",
						"N": "12345",
					},
					bson.M{
						"SS": []interface{}{"zi1", "zi2", "zi3"},
					},
				},
			},
			"test-inner-map": bson.M {
				"M": bson.M {
					"test": bson.M{
						"N": "12345000",
					},
				},
			},
			"test-NULL": bson.M {
				"NULL": false,
			},
		}, out.(RawData).Data, "should be equal")
	}
}

func TestTypeConverter(t *testing.T) {
	// test TypeConverter

	var nr int
	{
		fmt.Printf("TestTypeConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue{
			"test": {
				N: aws.String("12345"),
			},
		}

		rc := new(TypeConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		val, err := bson.ParseDecimal128("12345")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": val,
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestTypeConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue{
			"test": {
				N: aws.String("123456789101112131415161718192021"),
			},
			"test2": {
				N: aws.String("3.141592653589793238462643383279"),
			},
			"test3": {
				N: aws.String("3.1415926535897932384626433832795012345"),
			},
		}

		rc := new(TypeConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		val, err := bson.ParseDecimal128("123456789101112131415161718192021")
		assert.Equal(t, nil, err, "should be equal")
		val2, err := bson.ParseDecimal128("3.141592653589793238462643383279")
		assert.Equal(t, nil, err, "should be equal")
		val3, err := strconv.ParseFloat("3.1415926535897932384626433832795012345", 64)
		assert.Equal(t, nil, err, "should be equal")
		val3_2, err := bson.ParseDecimal128(fmt.Sprintf("%v", val3))
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": val,
			"test2": val2,
			"test3": val3_2,
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestTypeConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
			"fuck": {
				S: aws.String("hello"),
			},
		}

		rc := new(TypeConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		val, err := bson.ParseDecimal128("12345")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": val,
			"fuck": "hello",
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestTypeConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
			"fuck": {
				S: aws.String("hello"),
			},
			"test-string-list": {
				SS: []*string{aws.String("z1"), aws.String("z2"), aws.String("z3")},
			},
			"test-number-list": {
				NS: []*string{aws.String("123"), aws.String("456"), aws.String("789999999999")},
			},
			"test-bool": {
				BOOL: aws.Bool(true),
			},
			"test-byte": {
				B: []byte{123, 45, 78, 0, 12},
			},
			"test-byte-list": {
				BS: [][]byte{
					{123, 33, 44, 0, 55},
					{0, 1, 2, 0, 5},
				},
			},
		}

		rc := new(TypeConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		val, err := bson.ParseDecimal128("12345")
		assert.Equal(t, nil, err, "should be equal")
		val2, err := bson.ParseDecimal128("123")
		assert.Equal(t, nil, err, "should be equal")
		val3, err := bson.ParseDecimal128("456")
		assert.Equal(t, nil, err, "should be equal")
		val4, err := bson.ParseDecimal128("789999999999")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": val,
			"fuck": "hello",
			"test-string-list": []string{"z1", "z2", "z3"},
			"test-number-list": []bson.Decimal128{val2, val3, val4},
			"test-bool": true,
			"test-byte": []byte{123, 45, 78, 0, 12},
			"test-byte-list": [][]byte{
				{123, 33, 44, 0, 55},
				{0, 1, 2, 0, 5},
			},
		}, out.(RawData).Data, "should be equal")
	}

	{
		fmt.Printf("TestTypeConverter case %d.\n", nr)
		nr++

		src := map[string]*dynamodb.AttributeValue {
			"test": {
				N: aws.String("12345"),
			},
			"test-inner-struct": {
				L: []*dynamodb.AttributeValue {
					{
						S: aws.String("hello-inner"),
						// N: aws.String("12345"),
					},
					{
						SS: []*string{aws.String("zi1"), aws.String("zi2"), aws.String("zi3")},
					},
				},
			},
			"test-inner-map": {
				M: map[string]*dynamodb.AttributeValue{
					"test": {
						N: aws.String("12345000"),
					},
				},
			},
			"test-NULL": {
				NULL: aws.Bool(false),
			},
			"N": {
				M:map[string]*dynamodb.AttributeValue{
					"NN": {
						N: aws.String("567"),
					},
					"M": {
						S: aws.String("899"),
					},
				},
			},
		}

		rc := new(TypeConverter)
		out, err := rc.Run(src)
		assert.Equal(t, nil, err, "should be equal")
		val, err := bson.ParseDecimal128("12345")
		assert.Equal(t, nil, err, "should be equal")
		val2, err := bson.ParseDecimal128("12345000")
		assert.Equal(t, nil, err, "should be equal")
		val3, err := bson.ParseDecimal128("567")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.M{
			"test": val,
			"test-inner-struct": []interface{} {
				"hello-inner",
				[]string{"zi1", "zi2", "zi3"},
			},
			"test-inner-map": bson.M {
				"test": val2,
			},
			"test-NULL": false,
			"N": bson.M {
				"NN": val3,
				"M": "899",
			},
		}, out.(RawData).Data, "should be equal")
	}
}