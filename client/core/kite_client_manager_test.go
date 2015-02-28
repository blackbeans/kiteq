package core

import (
	"kiteq/binding"
	"kiteq/client/listener"
	"kiteq/protocol"
	"kiteq/server"
	"kiteq/store"
	"log"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
)

func buildStringMessage() *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(store.MessageId()),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

func TestNewManager(t *testing.T) {

	kiteQ := server.NewKiteQServer("127.0.0.1:13800", "localhost:2181", []string{"trade"}, "mock")
	kiteQ.Start()
	// 创建客户端
	manager := NewKiteClientManager("localhost:2181", "s-trade-a", "123456", &listener.MockListener{})
	manager.SetPublishTopics([]string{"trade"})

	// 设置接收类型
	manager.SetBindings(
		[]*binding.Binding{
			binding.Bind_Direct("s-trade-a", "trade", "pay-succ", 1000, true),
		},
	)

	manager.Start()

	// 发送数据
	err := manager.SendMessage(protocol.NewQMessage(buildStringMessage()))
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}

	// 发送数据
	err = manager.SendMessage(protocol.NewQMessage(buildStringMessage()))
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}

	// 发送数据
	err = manager.SendMessage(protocol.NewQMessage(buildStringMessage()))
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}

	time.Sleep(time.Second * 10)
	manager.Destory()
	kiteQ.Shutdown()

}
