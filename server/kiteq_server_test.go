package server

import (
	"github.com/golang/protobuf/proto"
	"kiteq/binding"
	"kiteq/client"
	// "kiteq/client/listener"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"testing"
	"time"
)

func buildStringMessage(id string) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:     proto.String("1234567_" + id),
		Topic:         proto.String("trade"),
		MessageType:   proto.String("pay-succ"),
		ExpiredTime:   proto.Int64(time.Now().Unix()),
		DeliveryLimit: proto.Int32(-1),
		GroupId:       proto.String("go-kite-test"),
		Commit:        proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

//初始化存储
var kitestore = &store.MockKiteStore{}
var ch = make(chan bool, 1)
var kiteClient *client.KiteQClient
var kiteQServer *KiteQServer

type defualtListener struct {
}

func (self *defualtListener) OnMessage(msg *protocol.StringMessage) bool {
	// log.Println("MockListener|OnMessage", *msg.Header, *msg.Body)
	return true
}

func (self *defualtListener) OnMessageCheck(messageId string, tx *protocol.TxResponse) error {
	// log.Println("MockListener|OnMessageCheck", messageId)
	tx.Commit()
	return nil
}

func init() {

	kiteQServer = NewKiteQServer("localhost:13800", "localhost:2181", []string{"trade"}, "mock")
	kiteQServer.Start()
	log.Println("KiteQServer START....")

	time.Sleep(5 * time.Second)
	kiteClient = client.NewKiteQClient("localhost:2181", "ps-trade-a", "123456", &defualtListener{})
	kiteClient.SetBindings([]*binding.Binding{
		binding.Bind_Direct("ps-trade-a", "trade", "pay-succ", 1000, true),
	})
	kiteClient.SetTopics([]string{"trade"})
	kiteClient.Start()
}

func BenchmarkRemotingServer(t *testing.B) {
	for i := 0; i < t.N; i++ {
		err := kiteClient.SendStringMessage(buildStringMessage("1"))
		if nil != err {
			t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		}
	}
}

func TestRemotingServer(t *testing.T) {

	err := kiteClient.SendStringMessage(buildStringMessage("1"))
	if nil != err {
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		t.Fail()

	}

	err = kiteClient.SendStringMessage(buildStringMessage("2"))
	if nil != err {
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		t.Fail()

	}

	err = kiteClient.SendStringMessage(buildStringMessage("3"))
	if nil != err {
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		t.Fail()

	}

	msg := buildStringMessage("4")
	msg.GetHeader().Commit = proto.Bool(false)

	err = kiteClient.SendStringMessage(msg)
	if nil != err {
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		t.Fail()

	}

	time.Sleep(20 * time.Second)
}
