package file

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"testing"
	"time"
)

func TestFileStoreQuery(t *testing.T) {
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(".", 5000000, 1*time.Second)
	fs.Start()

	for i := 0; i < 100; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))

		succ := fs.Save(entity)
		if !succ {
			t.Fail()
		}
	}

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"
		entity := fs.Query(id)
		if nil == entity {
			t.Fail()
			log.Printf("FAIL|%s\n", entity)
		} else {
			// log.Println(entity)
		}
	}
	fs.Stop()
	cleanSnapshot("./snapshot/")
}

func TestFileStoreCommit(t *testing.T) {
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(".", 5000000, 1*time.Second)
	fs.Start()

	for i := 0; i < 100; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))

		succ := fs.Save(entity)
		if !succ {
			t.Fail()
		}
	}

	//commit and check
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"
		fs.Commit(id)

		entity := fs.Query(id)
		if nil == entity {
			t.Fail()
		} else if !entity.Commit {
			t.Fail()
			// log.Println(entity)
		}
	}
	fs.Stop()
	cleanSnapshot("./snapshot/")
}

func TestFileStoreUpdate(t *testing.T) {
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(".", 5000000, 1*time.Second)
	fs.Start()

	for i := 0; i < 100; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(true),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))
		// log.Printf("------------%s", entity.Header)
		succ := fs.Save(entity)
		if !succ {
			t.Fail()
		}
	}

	//commit and check
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"

		//创建消息
		msg := &store.MessageEntity{
			MessageId:    id,
			DeliverCount: 1,
			SuccGroups:   []string{},
			FailGroups:   []string{"s-mts-test"}}

		succ := fs.UpdateEntity(msg)
		if !succ {
			t.Fail()
		}
		//check entity
		entity := fs.Query(id)
		// log.Printf("++++++++++++++|%s|%s", entity.Header, string(entity.GetBody().([]byte)))
		if nil == entity {
			t.Fail()
		} else if !entity.Commit && entity.DeliverCount != 1 &&
			entity.FailGroups[0] != "s-mts-test" {
			t.Fail()
			// log.Println(entity)
		}
	}
	fs.Stop()
	// cleanSnapshot("./snapshot/")
}

func TestFileStoreDelete(t *testing.T) {

	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(".", 5000000, 1*time.Second)
	fs.Start()

	for i := 0; i < 100; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(true),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))

		succ := fs.Save(entity)
		if !succ {
			t.Fail()
		}
	}

	//commit and check
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"

		//delete
		fs.Delete(id)

		//check entity
		entity := fs.Query(id)
		if nil != entity {
			t.Fail()
		}
	}
	fs.Stop()
	cleanSnapshot("./snapshot/")
}

func TestFileStoreInit(t *testing.T) {

	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(".", 5000000, 1*time.Second)
	fs.Start()

	for i := 0; i < 100; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(true),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))

		succ := fs.Save(entity)
		if !succ {
			t.Fail()
		}
	}

	fs.Stop()

	fs = NewKiteFileStore(".", 5000000, 1*time.Second)
	fs.Start()

	time.Sleep(10 * time.Second)
	//commit and check
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"

		//check entity
		entity := fs.Query(id)
		if nil == entity || !entity.Commit {
			log.Printf("TestFileStoreInit|FAIL|%s", id)
			t.Fail()
		}
	}
	fs.Stop()
	// cleanSnapshot("./snapshot/")
}
