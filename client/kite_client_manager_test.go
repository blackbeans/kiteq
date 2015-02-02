package client

import (
	"fmt"
	"kiteq/binding"
	"kiteq/client/listener"
	"kiteq/protocol"
	"log"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
)

func buildStringMessage() *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:   proto.String(messageId()),
		Topic:       proto.String("trade"),
		MessageType: proto.String("pay-succ"),
		ExpiredTime: proto.Int64(13700000000),
		GroupId:     proto.String("go-kite-test"),
		Commited:    proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

var f, _ = os.OpenFile("/dev/urandom", os.O_RDONLY, 0)

func messageId() string {
	b := make([]byte, 16)
	f.Read(b)
	return fmt.Sprintf("%x", b)
}

func TestNewManager(t *testing.T) {
	// 创建客户端
	manager := NewKiteClientManager(":18002", "", "s-trade-a", "123456")
	manager.AddListener(&listener.ConsoleListener{})
	// 设置发送类型
	if err := manager.SetPubs([]string{"trade"}); err != nil {
		log.Fatal(err)
	}
	// 设置接收类型
	if err := manager.SetSubs(
		[]*binding.Binding{
			binding.Bind_Direct("s-trade-a", "trade", "pay-succ", 1000, true),
		},
	); err != nil {
		log.Fatal(err)
	}

	// 发送数据
	err := manager.SendStringMessage(buildStringMessage())
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}

	// 发送数据
	err = manager.SendStringMessage(buildStringMessage())
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}

	// 发送数据
	err = manager.SendStringMessage(buildStringMessage())
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}

	time.Sleep(time.Second * 100)

}
