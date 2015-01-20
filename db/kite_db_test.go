package db

import (
	"fmt"
	"go-kite/store"
	"log"
	"testing"
	"time"
)

func TestSave(t *testing.T) {
	db := NewKiteDB("/Users/mengjun/dev/src/go-kite/data")
	session := db.GetSession()
	N := 1000
	begin := time.Now().UnixNano()
	for i := 0; i < N; i++ {
		session.Save(&store.MessageEntity{
			MessageId: fmt.Sprintf("%d", i),
			Topic:     "test",
			Body:      []byte(fmt.Sprintf("abc%d", i)),
		})
	}
	session.Flush("test")
	end := time.Now().UnixNano()
	log.Println("wait for async write")
	time.Sleep(time.Second * 5)
	for i := 0; i < N; i++ {
		entity := session.Query(fmt.Sprintf("%d", i))
		log.Println("query result", string(entity.Body))
	}
	log.Println("save ", N, "record use", (end-begin)/1000/1000, " ms")
}
