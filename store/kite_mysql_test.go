package store

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"os"
	"testing"
	"time"
)

func TestSave(t *testing.T) {

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

	entity := NewMessageEntity(protocol.NewQMessage(msg))
	entity.SuccGroups = []string{"go-kite-test"}

	kiteMysql := NewKiteMysql("root:@tcp(localhost:3306)/kite")
	kiteMysql.dbmap.DropTablesIfExists()

	succ := kiteMysql.Save(entity)
	if !succ {
		t.Fail()
	} else {
		t.Logf("SAVE|SUCC|%s\n", entity)
	}

	ret := kiteMysql.Query("26c03f00665862591f696a980b5a6c40")
	t.Logf("Query|%s\n", ret)
	if ret.GetBody().(string) != msg.GetBody() {
		t.Logf("Body not equals.|expeted:%s actual:%s\n", msg.GetBody(), ret.GetBody())
		t.Fail()
	}

	fmt.Println("Commint BEGIN")
	kiteMysql.Commit("26c03f00665862591f696a980b5a6c40")
	fmt.Println("Commint END")

	ret = kiteMysql.Query("26c03f00665862591f696a980b5a6c40")
	if !ret.Commit {
		t.Logf("Commit|FAIL|%s\n", ret)
		t.Fail()
	}

	hn, _ := os.Hostname()

	hasNext, entities := kiteMysql.PageQueryEntity("26c03f00665862591f696a980b5a6c40", hn, time.Now().Unix(), 0, 10)

	if hasNext {
		t.Logf("PageQueryEntity|FAIL|HasNext|%s\n", entities)
		t.Fail()
	} else {
		if len(entities) != 1 {
			t.Logf("PageQueryEntity|FAIL|%s\n", entities)
			t.Fail()
		} else {
			if entities[0].GetBody() != msg.GetBody() {
				t.Fail()
			} else {
				t.Logf("PageQueryEntity|SUCC|%s\n", entities)
			}
		}
	}

}
