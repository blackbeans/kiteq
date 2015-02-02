package server

import (
	"github.com/golang/protobuf/proto"
	"kiteq/binding"
	"kiteq/client"
	"kiteq/client/listener"
	"kiteq/handler"
	"kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"kiteq/store"
	"log"
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
func (self *defaultStore) Commit(messageId string) bool {
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

//初始化存储
// var kitestore = store.NewKiteMysql("root:root@tcp(localhost:8889)/kite")
var kitestore = &defaultStore{}
var ch = make(chan bool, 1)

// var kclient *core.KiteClient
var manager *client.KiteClientManager
var remotingServer *RemotingServer

func init() {

	//初始化pipeline
	pipeline := pipe.NewDefaultPipeline()
	clientManager := rclient.NewClientManager()
	exchanger := binding.NewBindExchanger("localhost:2181")

	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet"))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kitestore))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", exchanger))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver", kitestore))
	pipeline.RegisteHandler("remoting", handler.NewRemotingHandler("remoting", clientManager))

	remotingServer = NewRemotionServer("localhost:13801", 3*time.Second,
		func(remoteClient *rclient.RemotingClient, packet []byte) {
			event := pipe.NewPacketEvent(remoteClient, packet)
			err := pipeline.FireWork(event)
			if nil != err {
				log.Printf("RemotingServer|onPacketRecieve|FAIL|%s|%t\n", err, packet)
			}
		})

	go func() {

		err := remotingServer.ListenAndServer()
		if nil != err {
			ch <- false

		} else {
			ch <- true
		}
	}()

	exchanger.PushQServer("localhost:13801", []string{"trade"})

	time.Sleep(1 * time.Second)

	// 创建客户端
	manager = client.NewKiteClientManager(":18002", "", "s-trade-a", "123456")
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
	time.Sleep(1 * time.Second)
}

func BenchmarkRemotingServer(t *testing.B) {
	for i := 0; i < t.N; i++ {
		err := manager.SendStringMessage(buildStringMessage())
		if nil != err {
			t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		}
	}
}

func TestRemotingServer(t *testing.T) {

	err := manager.SendStringMessage(buildStringMessage())
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	err = manager.SendStringMessage(buildStringMessage())
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	err = manager.SendStringMessage(buildStringMessage())
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	time.Sleep(200 * time.Second)
}
