package listener

import (
	"kiteq/protocol"
	"log"
)

type IListener interface {
	OnMessage(msg *protocol.StringMessage) bool
	OnMessageCheck(messageId string) bool
}

type ConsoleListener struct {
}

func (self *ConsoleListener) OnMessage(msg *protocol.StringMessage) bool {
	log.Println("ConsoleListener|OnMessage", *msg.Header.MessageId, *msg.Body)
	return true
}

func (self *ConsoleListener) OnMessageCheck(messageId string) bool {
	log.Println("ConsoleListener|OnMessageCheck", messageId)
	return true
}
