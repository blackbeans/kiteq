package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/remoting/client"
	"log"
	"time"
)

//----------------鉴权handler
type ValidateHandler struct {
	BaseForwardHandler
	clientManager *client.ClientManager
}

//------创建鉴权handler
func NewValidateHandler(name string, clientManager *client.ClientManager) *ValidateHandler {
	ahandler := &ValidateHandler{}
	ahandler.BaseForwardHandler = NewBaseForwardHandler(name, ahandler)
	ahandler.clientManager = clientManager
	return ahandler
}

func (self *ValidateHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *ValidateHandler) cast(event IEvent) (val iauth, ok bool) {
	val, ok = event.(iauth)
	return val, ok
}

func (self *ValidateHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	aevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	remoteClient := aevent.getClient()
	//做权限校验.............
	isAuth := self.clientManager.Validate(remoteClient)
	// log.Printf("ValidateHandler|CONNETION|%s|%s\n", remoteClient.RemoteAddr(), isAuth)
	if isAuth {
		ctx.SendForward(event)
	} else {
		log.Printf("ValidateHandler|UnAuth CONNETION|%s\n", remoteClient.RemoteAddr())
		cmd := protocol.MarshalConnAuthAck(false, "未授权的访问,连接关闭!")
		//响应包
		packet := protocol.NewPacket(protocol.CMD_CONN_AUTH, cmd)

		//直接写出去授权失败
		remoteClient.WriteAndGet(*packet, 100*time.Millisecond)
		//断开连接
		remoteClient.Shutdown()
	}

	return nil

}
