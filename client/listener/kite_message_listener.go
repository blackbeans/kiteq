package listener

import (
	"kiteq/protocol"
	"log"
)

type IListener interface {
	//接受投递消息的回调
	OnMessage(msg *protocol.StringMessage) bool
	//接收事务回调
	// 除非明确提交成功、其余都为不成功
	// 有异常或者返回值为false均为不提交
	OnMessageCheck(messageId string, tx *protocol.TxResponse) error
}

type ConsoleListener struct {
}

func (self *ConsoleListener) OnMessage(msg *protocol.StringMessage) bool {
	log.Println("ConsoleListener|OnMessage", *msg.Header.MessageId, *msg.Body)
	return true
}

func (self *ConsoleListener) OnMessageCheck(messageId string, tx *protocol.TxResponse) error {
	log.Println("ConsoleListener|OnMessageCheck", messageId)
	tx.Commit()
	return nil
}
