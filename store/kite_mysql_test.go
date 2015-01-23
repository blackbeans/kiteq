package store

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func Benchmark_Save(b *testing.B) {
	db := NewKiteMysql("root:root@tcp(localhost:8889)/kite")
	e := &MessageEntity{
		topic: "test",
		body:  []byte("abc"),
	}
	var f, _ = os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
	bs := make([]byte, 16)

	for i := 0; i < b.N; i++ {
		f.Read(bs)
		e.messageId = fmt.Sprintf("%x", bs)
		db.Save(e)
	}
	log.Println("finish")
}

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
