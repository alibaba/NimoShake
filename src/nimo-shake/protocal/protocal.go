package protocal

import (
	utils "github.com/alibaba/NimoShake/src/nimo-shake/common"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// convert DynamoDB attributeValue to bson
type Converter interface {
	// run
	Run(input map[string]*dynamodb.AttributeValue) (interface{}, error)
}

func NewConverter(tp string) Converter {
	switch tp {
	case utils.ConvertTypeRaw:
		return new(RawConverter)
	case utils.ConvertTypeChange:
		return new(TypeConverter)
	case utils.ConvertTypeSame:
		return new(SameConverter)
	default:
		return nil
	}
}

type RawData struct {
	Size int         // fake size, only calculate real data
	Data interface{} // real data
}
