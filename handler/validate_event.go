package handler

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
)

//----------------鉴权handler
type ValidateHandler struct {
	turbo.BaseForwardHandler
	clientManager *turbo.ClientManager
}

//------创建鉴权handler
func NewValidateHandler(name string, clientManager *turbo.ClientManager) *ValidateHandler {
	ahandler := &ValidateHandler{}
	ahandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, ahandler)
	ahandler.clientManager = clientManager
	return ahandler
}

func (self *ValidateHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *ValidateHandler) cast(event turbo.IEvent) (val iauth, ok bool) {
	val, ok = event.(iauth)
	return val, ok
}

func (self *ValidateHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	aevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	c := aevent.getClient()
	//做权限校验.............
	isAuth := self.clientManager.Validate(c)
	// log.Debugf(  "ValidateHandler|CONNETION|%s|%s", client.RemoteAddr(), isAuth)
	if isAuth {
		ctx.SendForward(event)
	} else {
		log.Warnf("ValidateHandler|UnAuth CONNETION|%s", c.RemoteAddr())
		cmd := protocol.MarshalConnAuthAck(false, "Unauthorized,Connection broken!")
		//响应包
		p := turbo.NewPacket(protocol.CMD_CONN_AUTH, cmd)

		//直接写出去授权失败
		c.Write(*p)
		//断开连接
		c.Shutdown()
	}

	return nil

}
