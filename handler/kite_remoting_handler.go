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

	for _, session := range event.sessions {
		go func() {
			err := session.WriteReponse(event.packet)
			if nil != err {
				log.Printf("RemotingHandler|invokeGroup|%s|%t\n", err, event)
			}
		}()
	}

	return nil
}

//都低结果
type deliverResult struct {
	groupId string
	err     error
}
