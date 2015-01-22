package store

import (
	"log"
	"testing"
)

func TestQuery(t *testing.T) {
	db := NewKiteMysql("root:root@tcp(localhost:8889)/kite")
	// log.Println(db.Rollback("2"))
	e := db.Query("2")
	e.body = []byte("cba")
	db.UpdateEntity(e)
	log.Println(db.Query("2"))

	// log.Println(db.Save(&MessageEntity{
	// 	messageId: "2",
	// 	topic:     "test",
	// 	body:      []byte("abc"),
	// }))
}
