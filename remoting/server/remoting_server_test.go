package server

import (
	"github.com/golang/protobuf/proto"
	"kiteq/binding"
	"kiteq/client"
	"kiteq/client/chandler"
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
	entity := store.NewStringMessageEntity(buildStringMessage(messageId))
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

func buildStringMessage(id string) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:   proto.String("1234567_" + id),
		Topic:       proto.String("trade"),
		MessageType: proto.String("pay-succ"),
		ExpiredTime: proto.Int64(time.Now().Unix()),
		GroupId:     proto.String("go-kite-test"),
		Commited:    proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

//初始化存储
var kitestore = &defaultStore{}
var ch = make(chan bool, 1)
var kclient *client.KiteClient
var remotingServer *RemotingServer

func init() {

	initKiteServer()
	log.Println("KiteServer|START|....")
	initKiteClient()
	log.Println("KiteClient|START|....")
}

func initKiteServer() {
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

	remotingServer = NewRemotionServer("localhost:13800", 3*time.Second,
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

	exchanger.PushQServer("localhost:13800", []string{"trade"})
	//临时在这里创建的SessionManager
	time.Sleep(1 * time.Second)
}

func initKiteClient() {
	//开始向服务端发送数据

	clientm := rclient.NewClientManager()

	cpipe := pipe.NewDefaultPipeline()
	cpipe.RegisteHandler("kiteclient-packet", chandler.NewPacketHandler("kiteclient-packet"))
	cpipe.RegisteHandler("kiteclient-accept", chandler.NewAcceptHandler("kiteclient-accept"))
	cpipe.RegisteHandler("kiteclient-remoting", chandler.NewRemotingHandler("kiteclient-remoting", clientm))

	kclient = client.NewKitClient("user-service", "1234",
		"localhost:23800", "localhost:13800",
		func(remoteClient *rclient.RemotingClient, packet []byte) {
			event := pipe.NewPacketEvent(remoteClient, packet)
			err := cpipe.FireWork(event)
			if nil != err {
				log.Printf("KiteClient|onPacketRecieve|FAIL|%s|%t\n", err, packet)
			}
		})

	zk := binding.NewZKManager("localhost:2181")
	err := zk.PublishTopic([]string{"trade"}, "user-service", "localhost:23800")
	log.Printf("initKiteClient|PublishTopic|SUCC|%s...\n", err)
	err = zk.PublishBindings("user-service", []*binding.Binding{binding.Bind_Direct("user-service", "trade", "pay-succ", -1, true)})
	log.Println("initKiteClient|PublishBindings|SUCC|%s...\n", err)

	time.Sleep(1 * time.Second)
}

func BenchmarkRemotingServer(t *testing.B) {
	for i := 0; i < t.N; i++ {
		err := kclient.SendStringMessage(buildStringMessage("1"))
		if nil != err {
			t.Logf("SEND MESSAGE |FAIL|%s\n", err)
		}
	}
}

func TestRemotingServer(t *testing.T) {

	err := kclient.SendStringMessage(buildStringMessage("1"))
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	err = kclient.SendStringMessage(buildStringMessage("2"))
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	err = kclient.SendStringMessage(buildStringMessage("3"))
	if nil != err {
		t.Fail()
		t.Logf("SEND MESSAGE |FAIL|%s\n", err)
	}

	time.Sleep(20 * time.Second)
}
