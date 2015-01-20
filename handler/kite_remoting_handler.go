package handler

import (
	_ "log"
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

	revent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	// log.Printf("RemotingHandler|Process|%t\n", revent)

	//发送数据
	err := self.invokeGroup(revent)
	if nil == err {
		// ctx.SendForward()
	}
	return err
}

func (self *RemotingHandler) invokeSingle(event *RemotingEvent) error {
	return nil
}

func (self *RemotingHandler) invokeGroup(event *RemotingEvent) error {
	packet := event.packet.Marshal()
	for _, session := range event.sessions {
		//写到响应的channel中
		session.WriteChannel <- packet
		// log.Printf("RemotingHandler|invokeGroup|%t\n", packet)
	}

	return nil
}

//都低结果
type deliverResult struct {
	groupId string
	err     error
}
