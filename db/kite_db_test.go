package db

import (
	"go-kite/store"
	"testing"
	"time"
)

func TestSave(t *testing.T) {
	db := NewKiteDB("/Users/mengjun/dev/src/go-kite/data")
	session := db.GetSession()
	session.Save(&store.MessageEntity{
		MessageId: "1",
		Topic:     "test",
		Body:      []byte("abc"),
	})
	time.Sleep(time.Second * 3)
}
