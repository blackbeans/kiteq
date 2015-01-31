package client

import (
	"fmt"
	"kiteq/binding"
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
	// 设置发送类型
	if err := manager.SetPubs([]string{"trade"}); err != nil {
		t.Fatal(err)
	}
	// 设置接收类型
	if err := manager.SetSubs(
		[]*binding.Binding{
			binding.Bind_Direct("s-trade-a", "trade", "pay-succ", 1000, true),
		},
		// 这是从服务器投递过来的message
		func(msg *protocol.StringMessage) bool {
			log.Println("recv from server", msg)
			return true
		},
	); err != nil {
		t.Fatal(err)
	}
	// 构造发送请求
	msg := buildStringMessage()
	log.Println("msg entity: ", msg)
	// 发送数据
	err := manager.SendMessage(msg)
	if nil != err {
		log.Println("SEND MESSAGE |FAIL|", err)
	} else {
		log.Println("SEND MESSAGE |SUCCESS")
	}
	time.Sleep(time.Second * 10)

}
