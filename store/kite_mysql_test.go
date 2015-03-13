package store

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"os"
	"testing"
	"time"
)

func TestStringSave(t *testing.T) {

	//创建消息
	msg := &protocol.StringMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5a6c40"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}

	msg.Body = proto.String("hello world")
	innerT(msg, "26c03f00665862591f696a980b5a6c40", t)
}

func TestBytesSave(t *testing.T) {

	//创建消息
	msg := &protocol.BytesMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5a6c41"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}

	msg.Body = []byte("hello world")
	innerT(msg, "26c03f00665862591f696a980b5a6c41", t)
}

func innerT(msg interface{}, msgid string, t *testing.T) {
	qm := protocol.NewQMessage(msg)
	entity := NewMessageEntity(protocol.NewQMessage(msg))
	entity.SuccGroups = []string{"go-kite-test"}
	hn, _ := os.Hostname()
	entity.KiteServer = hn

	kiteMysql := NewKiteMysql("root:@tcp(localhost:3306)/kite")
	kiteMysql.dbmap.DropTablesIfExists()

	succ := kiteMysql.Save(entity)
	if !succ {
		t.Fail()
	} else {
		t.Logf("SAVE|SUCC|%s\n", entity)
	}

	ret := kiteMysql.Query(msgid)
	t.Logf("Query|%s\n", ret)
	bb, ok := qm.GetBody().([]byte)
	if ok {
		rb, _ := ret.GetBody().([]byte)
		if string(rb) != string(bb) {
			t.Fail()
		} else {
			t.Logf("PageQueryEntity|SUCC|%s\n", ret)
		}
	} else {
		bs, _ := qm.GetBody().(string)
		rs, _ := ret.GetBody().([]byte)
		if bs != string(rs) {
			t.Fail()
		} else {
			t.Logf("PageQueryEntity|SUCC|%s\n", ret)
		}
	}

	fmt.Println("Commint BEGIN")
	kiteMysql.Commit(msgid)
	fmt.Println("Commint END")

	ret = kiteMysql.Query(msgid)
	if !ret.Commit {
		t.Logf("Commit|FAIL|%s\n", ret)
		t.Fail()
	}

	hasNext, entities := kiteMysql.PageQueryEntity(msgid, hn, time.Now().Unix(), 0, 10)

	if hasNext {
		t.Logf("PageQueryEntity|FAIL|HasNext|%s\n", entities)
		t.Fail()
	} else {
		if len(entities) != 1 {
			t.Logf("PageQueryEntity|FAIL|%s\n", entities)
			t.Fail()
		} else {
			bb, ok := qm.GetBody().([]byte)
			if ok {
				rb, _ := entities[0].GetBody().([]byte)
				if string(rb) != string(bb) {
					t.Logf("PageQueryEntity|FAIL|%s|%s\n", string(rb), string(bb))
					t.Fail()
				} else {
					t.Logf("PageQueryEntity|SUCC|%s\n", entities[0])
				}
			} else {
				bs, _ := qm.GetBody().(string)
				rs, _ := entities[0].GetBody().([]byte)
				if bs != string(rs) {
					t.Logf("PageQueryEntity|FAIL|%s|%s\n", bs, entities[0].GetBody())
					t.Fail()
				} else {
					t.Logf("PageQueryEntity|SUCC|%s\n", entities[0])
				}
			}
		}
	}

}
