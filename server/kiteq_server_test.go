package server

import (
	"github.com/blackbeans/kiteq-common/binding"
	"github.com/blackbeans/kiteq-common/client"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/store"
	turbo "github.com/blackbeans/turbo"
	"github.com/golang/protobuf/proto"
	"log"
	"testing"
	"time"
)

func buildStringMessage(id string) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	mid := store.MessageId()
	mid = string(mid[:len(mid)-1]) + id
	// mid[len(mid)-1] = id[0]
	entity.Header = &protocol.Header{
		MessageId:    proto.String(mid),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true),
		Fly:          proto.Bool(false)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

type defualtListener struct {
}

func (self *defualtListener) OnMessage(msg *protocol.QMessage) bool {
	log.Printf("defualtListener|OnMessage|%s\n", msg.GetHeader().GetMessageId())
	return true
}

func (self *defualtListener) OnMessageCheck(tx *protocol.TxResponse) error {
	log.Printf("defualtListener|OnMessageCheck", tx.MessageId)
	tx.Commit()
	return nil
}

func BenchmarkRemotingServer(t *testing.B) {

	//初始化存储

	var kiteClient *client.KiteQClient
	var kiteQServer *KiteQServer
	var c int32 = 0
	var lc int32 = 0

	rc := turbo.NewRemotingConfig(
		"remoting-localhost:13800",
		2000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	kc := NewKiteQConfig(MockServerOption(), rc)

	kiteQServer = NewKiteQServer(kc)
	kiteQServer.Start()
	log.Println("KiteQServer START....")

	kiteClient = client.NewKiteQClient("localhost:2181", "s-trade-a", "123456", &defualtListener{})
	kiteClient.SetTopics([]string{"trade"})
	kiteClient.SetBindings([]*binding.Binding{
		binding.Bind_Direct("s-trade-a", "trade", "pay-succ", 1000, true),
	})
	kiteClient.Start()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Printf("%d\n", (c - lc))
			lc = c
		}
	}()

	t.SetParallelism(4)
	t.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := kiteClient.SendStringMessage(buildStringMessage("1"))
			if nil != err {
				t.Logf("SEND MESSAGE |FAIL|%s\n", err)
			}
		}
	})

	kiteClient.Destory()
	kiteQServer.Shutdown()
}

func TestRemotingServer(t *testing.T) {

	//初始化存储
	var kiteClient *client.KiteQClient
	var kiteQServer *KiteQServer
	var c int32 = 0
	var lc int32 = 0

	rc := turbo.NewRemotingConfig(
		"remoting-localhost:13800",
		2000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	kc := NewKiteQConfig(MockServerOption(), rc)

	kiteQServer = NewKiteQServer(kc)
	kiteQServer.Start()
	log.Println("KiteQServer START....")

	kiteClient = client.NewKiteQClient("localhost:2181", "s-trade-a", "123456", &defualtListener{})
	kiteClient.SetTopics([]string{"trade"})
	kiteClient.SetBindings([]*binding.Binding{
		binding.Bind_Direct("s-trade-a", "trade", "pay-succ", 1000, true),
	})
	kiteClient.Start()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Printf("%d\n", (c - lc))
			lc = c
		}
	}()

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

	time.Sleep(10 * time.Second)

	kiteClient.Destory()
	kiteQServer.Shutdown()
}
