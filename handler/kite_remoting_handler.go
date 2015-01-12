package handler

import (
	"log"
)

//远程操作的remotinghandler

type RemotingHandler struct {
	BaseForwardHandler
	IEventProcessor
}

func NewRemotingHandler(name string) *RemotingHandler {
	remtingHandler := &RemotingHandler{}
	remtingHandler.name = name
	remtingHandler.processor = remtingHandler
	return remtingHandler

}

func (self *RemotingHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *RemotingHandler) cast(event IEvent) (val *RemotingEvent, ok bool) {
	val, ok = event.(*RemotingEvent)
	return
}

func (self *RemotingHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	log.Printf("RemotingHandler|Process|%t\n", event)

	revent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//发送数据
	err := self.invokeGroup(revent)
	if nil == err {
		// ctx.SendForward()
	}
	return err
}

func (self *RemotingHandler) invokeGroup(event *RemotingEvent) error {
	return nil
}
