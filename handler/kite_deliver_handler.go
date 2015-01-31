package handler

import (
<<<<<<< HEAD
	// "github.com/golang/protobuf/proto"
	// "kiteq/protocol"
	"kiteq/remoting/session"
	"kiteq/store"
=======
	"kiteq/protocol"
	"kiteq/remoting/session"
	"kiteq/store"
	// "log"

	"github.com/golang/protobuf/proto"
>>>>>>> 45404355cf365ac9e1fb80d59a2659f669c4638b
)

//----------------持久化的handler
type DeliverHandler struct {
	BaseForwardHandler
	IEventProcessor
	sessionmanager *session.SessionManager
<<<<<<< HEAD
	store          store.IKiteStore
}

//------创建deliverpre
func NewDeliverHandler(name string, sessionmanager *session.SessionManager, store store.IKiteStore) *DeliverHandler {
=======
	kitestore      store.IKiteStore
}

//------创建deliverpre
func NewDeliverHandler(name string, sessionmanager *session.SessionManager, kitestore store.IKiteStore) *DeliverHandler {
>>>>>>> 45404355cf365ac9e1fb80d59a2659f669c4638b
	phandler := &DeliverHandler{}
	phandler.name = name
	phandler.processor = phandler
	phandler.sessionmanager = sessionmanager
<<<<<<< HEAD
	phandler.store = store
=======
	phandler.kitestore = kitestore
>>>>>>> 45404355cf365ac9e1fb80d59a2659f669c4638b
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
<<<<<<< HEAD
	// msgEntity := self.store.Query(pevent.MessageId)

	//发起一个请求包
=======
	entity := self.kitestore.Query(pevent.MessageId)
	// @todo 判断类型创建string或者byte message
	message := &protocol.StringMessage{}
	message.Header = entity.Header
	message.Body = proto.String(string(entity.GetBody()))
	data, err := proto.Marshal(message)
	if nil != err {
		return err
	}
>>>>>>> 45404355cf365ac9e1fb80d59a2659f669c4638b

	wpacket := &protocol.RequestPacket{
		CmdType: protocol.CMD_TYPE_STRING_MESSAGE,
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
