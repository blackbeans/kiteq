package chandler

import (
	"errors"
	"kiteq/client/listener"
	. "kiteq/pipe"
	"kiteq/protocol"
	"log"
)

//--------------------如下为具体的处理Handler
type AcceptHandler struct {
	BaseForwardHandler
	listener listener.IListener
}

func NewAcceptHandler(name string, listener listener.IListener) *AcceptHandler {
	ahandler := &AcceptHandler{}
	ahandler.BaseForwardHandler = NewBaseForwardHandler(name, ahandler)
	ahandler.listener = listener
	return ahandler
}

func (self *AcceptHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *AcceptHandler) cast(event IEvent) (val *AcceptEvent, ok bool) {
	val, ok = event.(*AcceptEvent)
	return
}

var INVALID_MSG_TYPE_ERROR = errors.New("INVALID MSG TYPE !")

func (self *AcceptHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {
	// log.Printf("AcceptHandler|Process|%s|%t\n", self.GetName(), event)

	acceptEvent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	switch acceptEvent.MsgType {
	case protocol.CMD_CHECK_MESSAGE:

		//回调事务完成的监听器
		log.Printf("AcceptHandler|Check Message|%t\n", acceptEvent.Msg)
		txPacket := acceptEvent.Msg.(*protocol.TxACKPacket)
		messageId := txPacket.GetMessageId()
		tx := protocol.NewTxResponse(messageId)
		err := self.listener.OnMessageCheck(messageId, tx)
		if nil != err {
			tx.Unknown(err.Error())
		}
		//发起一个向后的处理时间发送出去
		//填充条件
		tx.ConvertTxAckPacket(txPacket)

		txData, _ := protocol.MarshalPbMessage(txPacket)

		txResp := protocol.NewRespPacket(acceptEvent.Opaque, acceptEvent.MsgType, txData)

		//发送提交结果确认的Packet
		remotingEvent := NewRemotingEvent(txResp, []string{acceptEvent.RemoteClient.RemoteAddr()})
		ctx.SendForward(remotingEvent)

	case protocol.CMD_STRING_MESSAGE, protocol.CMD_BYTES_MESSAGE:
		//这里应该回调消息监听器然后发送处理结果
		log.Printf("AcceptHandler|Recieve Message|%t\n", acceptEvent.Msg)
		self.listener.OnMessage(acceptEvent.Msg.(*protocol.StringMessage))
	default:
		return INVALID_MSG_TYPE_ERROR
	}

	return nil

}
