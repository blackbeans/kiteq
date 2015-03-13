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
	if ret.GetBody().(string) != msg.GetBody() || ret.Commit {
		t.Fail()
	}

	fmt.Println("Commint BEGIN")
	kiteMysql.Commit("26c03f00665862591f696a980b5a6c40")
	fmt.Println("Commint END")

	ret = kiteMysql.Query("26c03f00665862591f696a980b5a6c40")
	if !ret.Commit {
		t.Fail()
	}

	hn, _ := os.Hostname()

	hasNext, entities := kiteMysql.PageQueryEntity("26c03f00665862591f696a980b5a6c40", hn, time.Now().Unix(), 0, 10)

	if !hasNext {
		t.Fail()
	} else {
		if len(entities) != 1 {
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

//func Benchmark_Save(b *testing.B) {
//	db := NewKiteMysql("root:root@tcp(localhost:8889)/kite")
//	e := &MessageEntity{
//		Topic: "test",
//		Body:  []byte("abc"),
//	}
//	var f, _ = os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
//	bs := make([]byte, 16)
//
//	for i := 0; i < b.N; i++ {
//		f.Read(bs)
//		e.MessageId = fmt.Sprintf("%x", bs)
//		db.Save(e)
//	}
//	log.Println("finish")
//}

func TestQuery(t *testing.T) {
	// db := NewKiteMysql("root:root@tcp(localhost:8889)/kite")
	// // log.Println(db.Rollback("2"))
	// e := db.Query("2")
	// e.body = []byte("cba")
	// db.UpdateEntity(e)
	// log.Println(db.Query("2"))

	// log.Println(db.Save(&MessageEntity{
	// 	messageId: "2",
	// 	topic:     "test",
	// 	body:      []byte("abc"),
	// }))
}
