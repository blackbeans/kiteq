package server

import (
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/binding"
	"kiteq/handler"
	"kiteq/stat"
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
	log.Printf("TestRecoverManager|-------------------%s\n", event)
	self.ch <- true
	return nil

}

func TestRecoverManager(t *testing.T) {

	pipeline := NewDefaultPipeline()

	kitedb := &store.MockKiteStore{}
	fs := stat.NewFlowStat("recover")

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger("localhost:2181", "127.0.0.1:13800")
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger, fs, 100))
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
