package checkpoint

import(
	"nimo-shake/common"

	LOG "github.com/vinllen/log4go"
)

type MongoWriter struct {
	address string
	conn *utils.MongoConn
	db string
}

func NewMongoWriter(address, db string) *MongoWriter {
	targetConn, err := utils.NewMongoConn(address, utils.ConnectModePrimary, true)
	if err != nil {
		LOG.Error("create mongodb connection error[%v]", err)
		return nil
	}

	return &MongoWriter{
		address: address,
		conn: targetConn,
		db: db,
	}
}

func (mw *MongoWriter) FindStatus(table string) (string, error) {

}