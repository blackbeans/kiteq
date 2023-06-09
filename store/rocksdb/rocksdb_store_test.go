package rocksdb

import (
	"context"
	"encoding/json"
	"github.com/blackbeans/go-uuid"
	"kiteq/store"
	"testing"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/golang/protobuf/proto"
)

var rocksstore *RocksDbStore

func init() {
	rocksstore = NewRocksDbStore(context.TODO(), ".", map[string]string{})
	rocksstore.Start()
}

func BenchmarkRocksDbStore_Save(b *testing.B) {
	for i := 0; i < b.N; i++ {
		//创建消息

		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(uuid.New()),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))
		if succ := rocksstore.Save(entity); !succ {
			b.FailNow()
		}
	}

}

func TestRocksDbStore_Save(t *testing.T) {

	//创建消息
	msg := &protocol.BytesMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5ac"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}
	msg.Body = []byte("hello world")

	entity := store.NewMessageEntity(protocol.NewQMessage(msg))
	if succ := rocksstore.Save(entity); !succ || *msg.Header.MessageId != "26c03f00665862591f696a980b5ac" {
		t.FailNow()
	}

	//查询消息
	entity = rocksstore.Query("trade", "26c03f00665862591f696a980b5ac")
	if nil == entity || entity.MessageId != "26c03f00665862591f696a980b5ac" {
		t.FailNow()
	}
	t.Logf("%+v", entity.MessageId)
}

func TestRocksDbStore_AsyncCommit(t *testing.T) {
	TestRocksDbStore_Save(t)

	//commited
	if commited := rocksstore.Commit("trade", "26c03f00665862591f696a980b5ac"); !commited {
		t.FailNow()
	}

	//查询消息
	entity := rocksstore.Query("trade", "26c03f00665862591f696a980b5ac")
	if nil == entity || entity.MessageId != "26c03f00665862591f696a980b5ac" || !entity.Commit {
		t.FailNow()
	}
}

//move to dlq
func TestRocksDbStore_Expired(t *testing.T) {
	TestRocksDbStore_Save(t)
	if succ := rocksstore.Expired("trade", "26c03f00665862591f696a980b5ac"); !succ {
		t.FailNow()
	}

	//查询消息
	entity := rocksstore.Query("trade", "26c03f00665862591f696a980b5ac")
	if nil != entity {
		t.FailNow()
	}
}

//更新消息
func TestRocksDbStore_AsyncUpdate(t *testing.T) {
	TestRocksDbStore_AsyncCommit(t)
	msg := &protocol.BytesMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5ac"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(24 * time.Hour).Unix()),
		DeliverLimit: proto.Int32(100),
	}

	now := time.Now().Add(20 * time.Minute).Unix()
	entity := store.NewMessageEntity(protocol.NewQMessage(msg))
	entity.FailGroups = []string{"s-vip-service"}
	entity.SuccGroups = []string{"s-profile-service", "s-group-service"}
	entity.DeliverCount = 11
	entity.NextDeliverTime = now

	//更新失败了？
	if succ := rocksstore.AsyncUpdateDeliverResult(entity); !succ {
		t.Logf("TestRocksDbStore_AsyncUpdate.AsyncUpdateDeliverResult|Fail|%v", entity)
		t.FailNow()
	}

	//查询消息
	entity = rocksstore.Query("trade", "26c03f00665862591f696a980b5ac")
	if nil == entity {
		t.Logf("TestRocksDbStore_AsyncUpdate.Query|Fail|26c03f00665862591f696a980b5ac")
		t.FailNow()
	}
	if entity.NextDeliverTime != now ||
		entity.FailGroups[0] != "s-vip-service" ||
		entity.SuccGroups[0] != "s-profile-service" || entity.SuccGroups[1] != "s-group-service" {
		t.Logf("TestRocksDbStore_AsyncUpdate.Query|Fail|%v", entity)
		t.FailNow()
	}
}

//
func TestRocksDbStore_PageQueryEntity(t *testing.T) {
	TestRocksDbStore_AsyncUpdate(t)

	hasmore, entities := rocksstore.PageQueryEntity("", "", time.Now().Add(30*time.Minute).Unix(), 0, 10)
	if hasmore || len(entities) != 1 {
		t.FailNow()
	}

	rawJson, _ := json.Marshal(entities)
	t.Logf("TestRocksDbStore_PageQueryEntity:%s", string(rawJson))

}
