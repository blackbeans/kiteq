package core

import (
	"github.com/blackbeans/turbo"
	"github.com/golang/protobuf/proto"
	"kiteq/binding"
	"kiteq/protocol"
	"kiteq/server"
	"kiteq/store"
	"log"
	"testing"
	"time"
)

func buildStringMessage(commit bool) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(store.MessageId()),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("ps-trade-a"),
		Commit:       proto.Bool(commit),
		Fly:          proto.Bool(false)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

func buildBytesMessage(commit bool) *protocol.BytesMessage {
	//创建消息
	entity := &protocol.BytesMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(store.MessageId()),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("ps-trade-a"),
		Commit:       proto.Bool(commit),
		Fly:          proto.Bool(false)}
	entity.Body = []byte("helloworld")

	return entity
}

type MockListener struct {
	rc  chan string
	txc chan string
}

func (self *MockListener) OnMessage(msg *protocol.QMessage) bool {
	log.Println("MockListener|OnMessage", msg.GetHeader(), msg.GetBody())
	self.rc <- msg.GetHeader().GetMessageId()

	return true
}

func (self *MockListener) OnMessageCheck(tx *protocol.TxResponse) error {
	log.Println("MockListener|OnMessageCheck", tx.MessageId)
	self.txc <- tx.MessageId
	tx.Commit()
	return nil
}

var rc = make(chan string, 1)
var txc = make(chan string, 1)
var kiteQ *server.KiteQServer
var manager *KiteClientManager

func init() {

	l := &MockListener{rc: rc, txc: txc}

	rc := turbo.NewRemotingConfig(
		"remoting-127.0.0.1:13800",
		2000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	kc := server.NewKiteQConfig(server.MockServerOption(), rc)
	kiteQ = server.NewKiteQServer(kc)

	// 创建客户端
	manager = NewKiteClientManager("localhost:2181", "ps-trade-a", "123456", l)
	manager.SetPublishTopics([]string{"trade"})

	// 设置接收类型
	manager.SetBindings(
		[]*binding.Binding{
			binding.Bind_Direct("ps-trade-a", "trade", "pay-succ", 1000, true),
		},
	)

	kiteQ.Start()
	time.Sleep(10 * time.Second)
	manager.Start()
}

func TestStringMesage(t *testing.T) {

	m := buildStringMessage(true)
	// 发送数据
	err := manager.SendMessage(protocol.NewQMessage(m))
	if nil != err {
		log.Println("SEND StringMESSAGE |FAIL|", err)
	} else {
		log.Println("SEND StringMESSAGE |SUCCESS")
	}

	select {
	case mid := <-rc:
		if mid != m.GetHeader().GetMessageId() {
			t.Fail()
		}
		log.Println("RECIEVE StringMESSAGE |SUCCESS")
	case <-time.After(10 * time.Second):
		log.Println("WAIT StringMESSAGE |TIMEOUT|", err)
		t.Fail()

	}

}

func TestBytesMessage(t *testing.T) {

	bm := buildBytesMessage(true)
	// 发送数据
	err := manager.SendMessage(protocol.NewQMessage(bm))
	if nil != err {
		log.Println("SEND BytesMESSAGE |FAIL|", err)
	} else {
		log.Println("SEND BytesMESSAGE |SUCCESS")
	}

	select {
	case mid := <-rc:
		if mid != bm.GetHeader().GetMessageId() {
			t.Fail()
		}
		log.Println("RECIEVE BytesMESSAGE |SUCCESS")
	case <-time.After(10 * time.Second):
		log.Println("WAIT BytesMESSAGE |TIMEOUT|", err)
		t.Fail()

	}

}

func TestTxBytesMessage(t *testing.T) {

	bm := buildBytesMessage(false)

	// 发送数据
	err := manager.SendTxMessage(protocol.NewQMessage(bm),
		func(message *protocol.QMessage) (bool, error) {
			return true, nil
		})
	if nil != err {
		log.Println("SEND TxBytesMESSAGE |FAIL|", err)
	} else {
		log.Println("SEND TxBytesMESSAGE |SUCCESS")
	}

	select {
	case mid := <-rc:
		if mid != bm.GetHeader().GetMessageId() {
			t.Fail()
		}
		log.Println("RECIEVE TxBytesMESSAGE |SUCCESS")
	case txid := <-txc:
		if txid != bm.GetHeader().GetMessageId() {
			t.Fail()
			log.Println("SEND TxBytesMESSAGE |RECIEVE TXACK SUCC")
		}

	case <-time.After(10 * time.Second):
		log.Println("WAIT TxBytesMESSAGE |TIMEOUT")
		t.Fail()

	}
}
