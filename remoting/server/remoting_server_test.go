package server

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"go-kite/client"
	"go-kite/handler"
	"go-kite/protocol"
	"go-kite/store"
	"testing"
	"time"
)

type defaultStore struct {
}

func (self *defaultStore) Query(messageId string) *store.MessageEntity {
	entity := store.NewStringMessageEntity(buildStringMessage())
	return entity

}
func (self *defaultStore) Save(entity *store.MessageEntity) bool {
	return true
}
func (self *defaultStore) Commite(messageId string) bool {
	return true
}
func (self *defaultStore) Rollback(messageId string) bool {
	return true
}
func (self *defaultStore) UpdateEntity(entity *store.MessageEntity) bool {
	return true
}

func buildStringMessage() *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:   proto.String("1234567"),
		Topic:       proto.String("trade"),
		MessageType: proto.String("pay-succ"),
		ExpiredTime: proto.Int64(13700000000),
		GroupId:     proto.String("go-kite-test"),
		Commited:    proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}
func TestRemotingServer(t *testing.T) {

	//初始化存储
	kitestore := &defaultStore{}

	//初始化pipeline
	pipeline := handler.NewDefaultPipeline()
	pipeline.RegisteHandler("packet_event", handler.NewPacketHandler("packet_event"))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kitestore))
	pipeline.RegisteHandler("remoting", handler.NewRemotingHandler("remoting"))
	fmt.Println(pipeline)
	ch := make(chan bool, 1)
	remotingServer := NewRemotionServer("localhost:13800", 3*time.Second, pipeline)
	go func() {

		err := remotingServer.ListenAndServer()
		//
		if nil != err {
			ch <- false
			t.Failed()
			t.Fatal("start remoting server fail!")
		} else {
			ch <- true
		}
	}()

	//开始向服务端发送数据
	client := client.NewKitClient("localhost:13800", "/user-service", "123456")
	client.HandShake()

	err := client.SendMessage(buildStringMessage())
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	err = client.SendMessage(buildStringMessage())
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	err = client.SendMessage(buildStringMessage())
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	client.Close()
	time.Sleep(10 * time.Second)
	remotingServer.Shutdonw()
}
