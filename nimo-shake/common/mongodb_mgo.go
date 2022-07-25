package utils

import (
	"time"
	"fmt"
	"reflect"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
	"github.com/jinzhu/copier"
)

const (
	ConnectModePrimary            = "primary"
	ConnectModeSecondaryPreferred = "secondaryPreferred"
	ConnectModeStandalone         = "standalone"

	OplogNS = "oplog.rs"
)

var (
	NotFountErr   = "not found"
	NsNotFountErr = "ns not found"
)

type NS struct {
	Database   string
	Collection string
}

func (ns NS) Str() string {
	return fmt.Sprintf("%s.%s", ns.Database, ns.Collection)
}

type MongoConn struct {
	Session *mgo.Session
	URL     string
}

func NewMongoConn(url string, connectMode string, timeout bool) (*MongoConn, error) {
	if connectMode == ConnectModeStandalone {
		url += "?connect=direct"
	}

	session, err := mgo.Dial(url)
	if err != nil {
		LOG.Critical("Connect to [%s] failed. %v", url, err)
		return nil, err
	}
	// maximum pooled connections. the overall established sockets
	// should be lower than this value(will block otherwise)
	session.SetPoolLimit(256)
	if timeout {
		session.SetSocketTimeout(10 * time.Minute)
	} else {
		session.SetSocketTimeout(0)
	}

	// already ping in the session
	/*if err := session.Ping(); err != nil {
		LOG.Critical("Verify ping command to %s failed. %v", url, err)
		return nil, err
	}*/

	// Switch the session to a eventually behavior. In that case session
	// may read for any secondary node. default mode is mgo.Strong
	switch connectMode {
	case ConnectModePrimary:
		session.SetMode(mgo.Primary, true)
	case ConnectModeSecondaryPreferred:
		session.SetMode(mgo.SecondaryPreferred, true)
	case ConnectModeStandalone:
		session.SetMode(mgo.Monotonic, true)
	default:
		err = fmt.Errorf("unknown connect mode[%v]", connectMode)
		return nil, err
	}

	LOG.Info("New session to %s successfully", url)
	return &MongoConn{Session: session, URL: url}, nil
}

func (conn *MongoConn) Close() {
	LOG.Info("Close session with %s", conn.URL)
	conn.Session.Close()
}

func (conn *MongoConn) IsGood() bool {
	if err := conn.Session.Ping(); err != nil {
		return false
	}

	return true
}

func (conn *MongoConn) AcquireReplicaSetName() string {
	var replicaset struct {
		Id string `bson:"set"`
	}
	if err := conn.Session.DB("admin").Run(bson.M{"replSetGetStatus": 1}, &replicaset); err != nil {
		LOG.Warn("Replica set name not found in system.replset, %v", err)
	}
	return replicaset.Id
}

func (conn *MongoConn) HasOplogNs() bool {
	if ns, err := conn.Session.DB("local").CollectionNames(); err == nil {
		for _, table := range ns {
			if table == OplogNS {
				return true
			}
		}
	}
	return false
}

func (conn *MongoConn) HasUniqueIndex() bool {
	checkNs := make([]NS, 0, 128)
	var databases []string
	var err error
	if databases, err = conn.Session.DatabaseNames(); err != nil {
		LOG.Critical("Couldn't get databases from remote server %v", err)
		return false
	}

	for _, db := range databases {
		if db != "admin" && db != "local" {
			coll, _ := conn.Session.DB(db).CollectionNames()
			for _, c := range coll {
				if c != "system.profile" {
					// push all collections
					checkNs = append(checkNs, NS{Database: db, Collection: c})
				}
			}
		}
	}

	for _, ns := range checkNs {
		indexes, _ := conn.Session.DB(ns.Database).C(ns.Collection).Indexes()
		for _, idx := range indexes {
			// has unique index
			if idx.Unique {
				LOG.Info("Found unique index %s on %s.%s in auto shard mode", idx.Name, ns.Database, ns.Collection)
				return true
			}
		}
	}

	return false
}

// first is from dynamo, second is from mongo
func CompareBson(first, second bson.M) (bool, error) {
	v2 := make(bson.M, 0)
	if err := copier.Copy(&v2, &second); err != nil {
		return false, fmt.Errorf("copy[%v] failed[%v]", second, err)
	}

	if _, ok := v2["_id"]; ok {
		delete(v2, "_id")
	}

	return reflect.DeepEqual(first, v2), nil
	// return DeepEqual(first, v2), nil
}
