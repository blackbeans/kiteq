package pipe

import (
	"log"
	"testing"
)

type mockForwardEvent struct {
	IForwardEvent
}

type mockForwarkHandler struct {
	BaseForwardHandler
	transport chan string
}

func (self *mockForwarkHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *mockForwarkHandler) cast(event IEvent) (val *mockForwardEvent, ok bool) {
	val, ok = event.(*mockForwardEvent)
	return
}

func (self *mockForwarkHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	self.transport <- self.GetName()
	log.Println("mockForwarkHandler|Process....")
	//向后走网络传输
	ctx.SendForward(event)
	return nil

}

type mockBackwardEvent struct {
	IBackwardEvent
}

type mockBackwardHandler struct {
	BaseBackwardHandler
	transport chan string
}

func (self *mockBackwardHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *mockBackwardHandler) cast(event IEvent) (val *mockBackwardEvent, ok bool) {
	val, ok = event.(*mockBackwardEvent)
	return
}

func (self *mockBackwardHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	self.transport <- self.GetName()
	log.Println("mockBackwardHandler|Process....")
	//向后走网络传输
	// ctx.SendForward(&mockForwardEvent{})
	return nil
}

type mockDoubleSideHandler struct {
	BaseDoubleSidedHandler
	transport chan string
}

func (self *mockDoubleSideHandler) TypeAssert(event IEvent) bool {
	ok := self.cast(event)
	return ok
}

func (self *mockDoubleSideHandler) cast(event IEvent) bool {
	_, okb := event.(*mockBackwardEvent)
	_, okf := event.(*mockForwardEvent)
	return okb || okf
}

func (self *mockDoubleSideHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	self.transport <- self.GetName()
	log.Println("mockDoubleSideHandler|Process....")
	//向后走网络传输
	ctx.SendBackward(&mockBackwardEvent{})
	return nil
}

//测试一下context
func TestPipelineContext(t *testing.T) {
	transport := make(chan string, 10)
	pipe := NewDefaultPipeline()
	mfh := &mockForwarkHandler{transport: transport}
	mfh.BaseForwardHandler = NewBaseForwardHandler("mockForwarkHandler", mfh)
	pipe.RegisteHandler("mockForwarkHandler", mfh)

	mbh := &mockBackwardHandler{transport: transport}
	mbh.BaseBackwardHandler = NewBaseBackwardHandler("mockBackwardHandler", mbh)
	pipe.RegisteHandler("mockBackwardHandler", mbh)

	mdh := &mockDoubleSideHandler{transport: transport}
	mdh.BaseDoubleSidedHandler = NewBaseDoubleSidedHandler("mockDoubleHandler", mdh)
	pipe.RegisteHandler("mockDoubleHandler", mdh)

	pipe.FireWork(&mockForwardEvent{})

	seq := []string{"mockForwarkHandler", "mockDoubleHandler", "mockBackwardHandler"}
	for i := 0; i < 3; i++ {
		select {

		case h := <-transport:

			if h == seq[i] && len(seq) > 0 {
				log.Printf("TRACE|%s....\n", h)
			} else {
				t.Fail()
			}
		default:
			t.Fail()
		}

	}

}
