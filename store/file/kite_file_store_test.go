package file

import (
	"context"
	"fmt"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/golang/protobuf/proto"
	"kiteq/store"
	"testing"
	"time"
)

func TestFileStoreQuery(t *testing.T) {
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
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
		entity := fs.Query("trade", id)
		if nil == entity {
			t.Fail()
			log.Printf("FAIL|%v", entity)
		} else {
			// log.Println(entity)
		}
	}
	fs.Stop()
	cleanSnapshot("./snapshot/")
}

func BenchmarkTestFileAppend(t *testing.B) {
	t.StopTimer()
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
	fs.Start()
	t.StartTimer()
	for i := 0; i < t.N; i++ {
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

}

func TestFileDuplicateAppend(t *testing.T) {
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
	fs.Start()
	for i := 0; i < 10; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5ac"),
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
		if i == 0 && !succ {
			t.Fail()
			return
		}

		if i > 0 && succ {
			return
		}
	}

}

func TestFileStoreCommit(t *testing.T) {
	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
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
		fs.Commit("trade", id)

		entity := fs.Query("trade", id)
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
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
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
		entity := fs.Query("trade", id)
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
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
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
		fs.Delete("trade", id)

	}

	time.Sleep(5 * time.Second)
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"
		//check entity
		entity := fs.Query("trade", id)
		if nil != entity {
			t.Fail()
		}
	}

	fs.Stop()
	cleanSnapshot("./snapshot/")
}

func TestFileStoreInit(t *testing.T) {

	cleanSnapshot("./snapshot/")
	fs := NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
	fs.Start()

	for i := 0; i < 100; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprint(i) + "26c03f00665862591f696a980b5ac"),
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

		if i < 50 {
			fs.AsyncDelete(entity.Topic, entity.MessageId)
		}
	}

	time.Sleep(10 * time.Second)
	fs.Stop()

	log.Println("-------------------Query")
	fs = NewKiteFileStore(context.TODO(), ".", 1000, 5000000, 1*time.Second)
	fs.Start()

	// for _, v := range fs.oplogs {
	// 	for _, e := range v {
	// 		ob := e.Value.(*opBody)
	// 		log.Printf("TestFileStoreInit|Check|%d|%s", ob.Id, ob.MessageId)
	// 	}
	// }
	log.Printf("TestFileStoreInit|Check|SUCC|")
	//commit and check
	for i := 50; i < 100; i++ {
		id := fmt.Sprint(i) + "26c03f00665862591f696a980b5ac"

		//check entity
		entity := fs.Query("trade", id)
		if nil == entity || !entity.Commit {
			// log.Printf("TestFileStoreInit|Exist|FAIL|%s|%s", id, entity)
			t.Fail()
			return
		}
		log.Printf("TestFileStoreInit|Exist|SUCC|%d|%s|%v", i, id, entity)
	}

	log.Printf("TestFileStoreInit|Exist|")

	//commit and check
	for i := 0; i < 50; i++ {
		id := fmt.Sprint(i) + "26c03f00665862591f696a980b5ac"

		//check entity
		entity := fs.Query("trade", id)
		if nil != entity {
			log.Printf("TestFileStoreInit|Delete|FAIL|%s", id)
			t.Fail()
		}
	}

	log.Printf("TestFileStoreInit|Delete")
	fs.Stop()
	cleanSnapshot("./snapshot/")
}
