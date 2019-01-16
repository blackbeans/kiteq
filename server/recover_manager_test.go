package server

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/turbo"
	"github.com/golang/protobuf/proto"
	"kiteq/exchange"
	"kiteq/handler"
	"kiteq/store"
	"kiteq/store/memory"
	"log"
	"os"
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

type mockDeliverHandler struct {
	turbo.BaseDoubleSidedHandler
	ch chan bool
}

func newmockDeliverHandler(name string, ch chan bool) *mockDeliverHandler {

	phandler := &mockDeliverHandler{}
	phandler.BaseDoubleSidedHandler = turbo.NewBaseDoubleSidedHandler(name, phandler)
	phandler.ch = ch
	return phandler
}

func (self *mockDeliverHandler) TypeAssert(event turbo.IEvent) bool {
	return true
}

func (self *mockDeliverHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {
	log.Printf("TestRecoverManager|-------------------%s\n", event)
	self.ch <- true
	return nil

}

func TestRecoverManager(t *testing.T) {

	pipeline := turbo.NewDefaultPipeline()

	kitedb := memory.NewKiteMemoryStore(100, 100)

	messageid := store.MessageId()
	t.Logf("messageid:%s\b", messageid)
	entity := store.NewMessageEntity(protocol.NewQMessage(buildStringMessage(messageid)))
	kitedb.Save(entity)
	go func() {
		for {
			log.Println(kitedb.Monitor())
			time.Sleep(1 * time.Second)
		}
	}()

	fs := stat.NewFlowStat()
	ch := make(chan bool, 1)

	// 临时在这里创建的BindExchanger
	exchanger := exchange.NewBindExchanger("zk://localhost:2181", "127.0.0.1:13800")

	tw := turbo.NewTimerWheel(100*time.Millisecond, 10)

	deliveryRegistry := handler.NewDeliveryRegistry(tw, 10)
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger, fs, 100, deliveryRegistry))
	pipeline.RegisteHandler("deliver", newmockDeliverHandler("deliver", ch))
	hostname, _ := os.Hostname()
	tw := turbo.NewTimerWheel(100*time.Millisecond, 10)
	rm := NewRecoverManager(hostname, 16*time.Second, pipeline, kitedb, tw)
	rm.Start()
	select {
	case succ := <-ch:
		log.Printf("--------------recover %s\n", succ)
	case <-time.After(5 * time.Second):
		t.Fail()
		log.Println("waite recover  deliver timeout\n")
	}

}
