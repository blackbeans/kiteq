package kite

import (
	"go-kite/store"
	"go-kite/store/kite"
	"testing"
)

func TestSave(t *testing.T) {
	db := NewKiteDB("/Users/mengjun/dev/go-kite/data")
	session := db.GetSession()
	session.Save(&MessageEntity{
		messageId: 1,
		Topic:     "test",
		body:      []byte("abc"),
	})
}
