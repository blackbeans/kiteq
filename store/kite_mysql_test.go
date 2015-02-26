package store

import (
	"fmt"
	"testing"
)

func TestSave(t *testing.T) {
	kiteMysql := NewKiteMysql("root:@/kite")
	kiteMysql.Save(&MessageEntity{
		MessageId: "1",
		Topic:     "test",
		Body:      []byte("abc"),
	})

	kiteMysql.Save(&MessageEntity{
		MessageId: "sutao",
		Topic:     "test222",
		Body:      []byte("abc222:w2"),
	})

	ret := kiteMysql.Query("sutao")
	fmt.Println(ret)

	kiteMysql.Commit("sutao")
	kiteMysql.Delete("sutao")
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
