package mysql

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/golang/protobuf/proto"
	"kiteq/store"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestConvertFields(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  1000,
		BatchDelSize: 1000,
		FlushPeriod:  1 * time.Minute,
		MaxIdleConn:  2,
		MaxOpenConn:  4}

	hs := newDbShard(options)

	c := convertor{}
	sqlwrapper := newSqlwrapper("kite_msg", hs, store.MessageEntity{})
	sqlwrapper.initSQL()
	c.columns = sqlwrapper.columns

	entity := &store.MessageEntity{}
	fs := c.convertFields(entity, func(colname string) bool {
		return false
	})
	t.Logf("%d|%s\n", len(c.columns), fs)
	if len(fs) != len(c.columns) {
		t.Fail()
	}
}

func TestConvert2Entity(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  1000,
		BatchDelSize: 1000,
		FlushPeriod:  1 * time.Minute,
		MaxIdleConn:  2,
		MaxOpenConn:  4}

	hs := newDbShard(options)

	c := convertor{}
	sqlwrapper := newSqlwrapper("kite_msg", hs, store.MessageEntity{})
	sqlwrapper.initSQL()
	c.columns = sqlwrapper.columns

	//创建消息
	msg := &protocol.BytesMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665462591f696a980b5a6c4"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}
	msg.Body = []byte("hello world")

	entity := store.NewMessageEntity(protocol.NewQMessage(msg))
	entity.SuccGroups = []string{"go-kite-test"}
	hn, _ := os.Hostname()
	entity.KiteServer = hn

	params := c.Convert2Params(entity)
	t.Logf("TestConvert2Entity|Convert2Params|%s\n", params)
	econ := &store.MessageEntity{}
	c.Convert2Entity(params, econ, func(colname string) bool {
		return false
	})
	t.Logf("TestConvert2Entity|Convert2Entity|%s\n", econ)
	if econ.MessageId != entity.MessageId {
		t.Fail()
	}

	if econ.ExpiredTime != entity.ExpiredTime {
		t.Fail()
	}

}

func TestConvert2Params(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  1000,
		BatchDelSize: 1000,
		FlushPeriod:  1 * time.Minute,
		MaxIdleConn:  2,
		MaxOpenConn:  4}

	hs := newDbShard(options)

	c := convertor{}
	sqlwrapper := newSqlwrapper("kite_msg", hs, store.MessageEntity{})
	sqlwrapper.initSQL()
	c.columns = sqlwrapper.columns

	//创建消息
	msg := &protocol.BytesMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5a6c4"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}
	msg.Body = []byte("hello world")

	entity := store.NewMessageEntity(protocol.NewQMessage(msg))
	entity.SuccGroups = []string{"go-kite-test"}
	hn, _ := os.Hostname()
	entity.KiteServer = hn

	params := c.Convert2Params(entity)

	if nil != params {
		for i, col := range c.columns {
			cv := params[i]
			t.Logf("TestConvert2Params|FIELD|%s|%s\n", col.fieldName, cv)
			if col.fieldName == "MessageId" {
				rv := reflect.ValueOf(cv)
				s := rv.Elem().Interface()
				if s.(string) != entity.MessageId {
					t.Fail()
				}
			} else if col.columnName == "body" {
				rv := reflect.ValueOf(cv)
				s := rv.Elem().Interface()
				if string(s.([]byte)) != string(entity.GetBody().([]byte)) {
					t.Fail()
				}
			}
		}
	} else {
		t.Fail()
	}
}
