package server

import (
	"kiteq/binding"
	"kiteq/handler"
	. "kiteq/pipe"
	"kiteq/store"
	"log"
	"os"
	"testing"
	"time"
)

type mockDeliverHandler struct {
	BaseDoubleSidedHandler
	ch chan bool
}

func newmockDeliverHandler(name string, ch chan bool) *mockDeliverHandler {

	phandler := &mockDeliverHandler{}
	phandler.BaseDoubleSidedHandler = NewBaseDoubleSidedHandler(name, phandler)
	phandler.ch = ch
	return phandler
}

func (self *mockDeliverHandler) TypeAssert(event IEvent) bool {
	return true
}

func (self *mockDeliverHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {
	log.Printf("-------------------%s\n", event)
	self.ch <- true
	return nil

}

func TestRecoverManager(t *testing.T) {

	pipeline := NewDefaultPipeline()

	kitedb := &store.MockKiteStore{}

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger("localhost:2181", "127.0.0.1:13800")
	ch := make(chan bool, 1)

	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger))
	pipeline.RegisteHandler("deliver", newmockDeliverHandler("deliver", ch))
	hostname, _ := os.Hostname()
	rm := NewRecoverManager(hostname, 1*time.Second, pipeline, kitedb)
	rm.Start()
	select {
	case succ := <-ch:
		log.Printf("--------------recover %s\n", succ)
	case <-time.After(20 * time.Second):
		t.Fail()
		log.Println("waite recover  deliver timeout\n")
	}

}
