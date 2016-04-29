package server

import (
	"github.com/blackbeans/kiteq-common/binding"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/kiteq-common/store"
	"github.com/blackbeans/kiteq-common/store/memory"
	"github.com/blackbeans/turbo"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/handler"
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

	fs := stat.NewFlowStat("recover")
	ch := make(chan bool, 1)

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger("localhost:2181", "127.0.0.1:13800")
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger, fs, 100))
	pipeline.RegisteHandler("deliver", newmockDeliverHandler("deliver", ch))
	hostname, _ := os.Hostname()
	tw := turbo.NewTimeWheel(100*time.Millisecond, 10, 10)
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
