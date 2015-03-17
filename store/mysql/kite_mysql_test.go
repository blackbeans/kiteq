package mysql

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"kiteq/store"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {

	kiteMysql := NewKiteMysql("root:@tcp(localhost:3306)/kite", 100, 100, 10*time.Millisecond)
	cerr := kiteMysql.dbmap.CreateTablesIfNotExists()
	if nil != cerr {
		t.Logf("TestBatch|CreateTablesIfNotExists|FAIL|%s\n", cerr)
	}

	err := kiteMysql.dbmap.TruncateTables()
	if nil != err {
		t.Logf("TestBatch|TruncateTables|FAIL|%s\n", err)
	}

	mids := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5a6c4" + strconv.Itoa(i)),
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
		kiteMysql.Save(entity)
		mids = append(mids, entity.MessageId)
	}

	for _, v := range mids {

		msg := &store.MessageEntity{
			MessageId:    v,
			DeliverCount: 1,
			SuccGroups:   []string{"s-mts-test"},
			FailGroups:   []string{},
			//设置一下下一次投递时间
			NextDeliverTime: time.Now().Unix()}
		kiteMysql.AsyncUpdate(msg)
	}

	time.Sleep(5 * time.Second)
	for _, v := range mids {
		entity := kiteMysql.Query(v)
		if len(entity.SuccGroups) < 1 {
			t.Fatalf("TestBatch|COMMIT FAIL|%s\n", entity)
			t.Fail()
			return
		}
	}

	//测试批量删除
	for _, v := range mids {
		kiteMysql.AsyncDelete(v)
	}
	time.Sleep(5 * time.Second)
	for _, v := range mids {
		entity := kiteMysql.Query(v)
		if nil != entity {
			t.Fatalf("TestBatch|AysncDelete FAIL|%s\n", entity)
			t.Fail()

		}
	}

	kiteMysql.dbmap.TruncateTables()
}

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
	entity := store.NewMessageEntity(protocol.NewQMessage(msg))
	entity.SuccGroups = []string{"go-kite-test"}
	hn, _ := os.Hostname()
	entity.KiteServer = hn

	kiteMysql := NewKiteMysql("root:@tcp(localhost:3306)/kite", 100, 100, 6*time.Second)
	cerr := kiteMysql.dbmap.CreateTablesIfNotExists()
	if nil != cerr {
		t.Logf("innerT|CreateTablesIfNotExists|FAIL|%s\n", cerr)
	}

	err := kiteMysql.dbmap.TruncateTables()
	if nil != err {
		t.Logf("innerT|TruncateTables|FAIL|%s\n", err)
	}

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
		rs, _ := ret.GetBody().(string)
		if bs != rs {
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
	t.Logf("PageQueryEntity|%s\n", entities)
	if hasNext {
		t.Logf("PageQueryEntity|FAIL|HasNext|%s\n", entities)
		t.Fail()
	} else {
		if len(entities) != 1 {
			t.Logf("PageQueryEntity|FAIL|%s\n", entities)
			t.Fail()
		} else {
			if entities[0].Header.GetMessageId() != qm.GetHeader().GetMessageId() {
				t.Fail()
			}
		}
	}

}
