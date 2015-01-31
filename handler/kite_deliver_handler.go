package handler

import (
	// "github.com/golang/protobuf/proto"
	// "kiteq/protocol"

	"kiteq/protocol"
	"kiteq/remoting/session"
	"kiteq/store"
	// "log"

	"github.com/golang/protobuf/proto"
)

//----------------持久化的handler
type DeliverHandler struct {
	BaseForwardHandler
	IEventProcessor
	sessionmanager *session.SessionManager
	kitestore      store.IKiteStore
}

//------创建deliverpre
func NewDeliverHandler(name string, sessionmanager *session.SessionManager, kitestore store.IKiteStore) *DeliverHandler {

	phandler := &DeliverHandler{}
	phandler.name = name
	phandler.processor = phandler
	phandler.sessionmanager = sessionmanager

	phandler.kitestore = kitestore
	return phandler
}

func (self *DeliverHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverHandler) cast(event IEvent) (val *DeliverEvent, ok bool) {
	val, ok = event.(*DeliverEvent)
	return
}

func (self *DeliverHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("DeliverHandler|Process|%t\n", event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//查找分组对应的session
	sessions := self.sessionmanager.FindSessions(pevent.DeliverGroups, func(s *session.Session) bool {
		return false
	})

	//获取投递的消息数据

	//发起一个请求包

	entity := self.kitestore.Query(pevent.MessageId)
	// @todo 判断类型创建string或者byte message
	message := &protocol.StringMessage{}
	message.Header = entity.Header
	message.Body = proto.String(string(entity.GetBody()))
	data, err := proto.Marshal(message)
	if nil != err {
		return err
	}

	wpacket := &protocol.Packet{
		CmdType: protocol.CMD_STRING_MESSAGE,
		Data:    data,
		// @todo 生成序号如何生成
		Opaque: 1,
	}
	//创建投递事件
	revent := newRemotingEvent(wpacket, sessions...)

	//向后转发
	ctx.SendForward(revent)

	return nil

}
