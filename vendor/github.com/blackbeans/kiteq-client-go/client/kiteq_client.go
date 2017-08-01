package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry/bind"
)

type KiteQClient struct {
	kclientManager *KiteClientManager
}

func (self *KiteQClient) Start() {
	self.kclientManager.Start()
}

func NewKiteQClient(zkAddr, groupId, secretKey string, listener IListener) *KiteQClient {
	return NewKiteQClientWithWarmup(zkAddr, groupId, secretKey, 0, listener)
}

func NewKiteQClientWithWarmup(zkAddr, groupId, secretKey string, warmingupSec int, listener IListener) *KiteQClient {
	return &KiteQClient{
		kclientManager: NewKiteClientManager(zkAddr, groupId, secretKey, warmingupSec, listener)}
}

func (self *KiteQClient) SetTopics(topics []string) {
	self.kclientManager.SetPublishTopics(topics)
}

func (self *KiteQClient) SetBindings(bindings []*bind.Binding) {
	self.kclientManager.SetBindings(bindings)

}

func (self *KiteQClient) SendTxStringMessage(msg *protocol.StringMessage, transcation DoTranscation) error {
	message := protocol.NewQMessage(msg)
	return self.kclientManager.SendTxMessage(message, transcation)
}

func (self *KiteQClient) SendTxBytesMessage(msg *protocol.BytesMessage, transcation DoTranscation) error {
	message := protocol.NewQMessage(msg)
	return self.kclientManager.SendTxMessage(message, transcation)
}

func (self *KiteQClient) SendStringMessage(msg *protocol.StringMessage) error {
	message := protocol.NewQMessage(msg)
	return self.kclientManager.SendMessage(message)
}

func (self *KiteQClient) SendBytesMessage(msg *protocol.BytesMessage) error {
	message := protocol.NewQMessage(msg)
	return self.kclientManager.SendMessage(message)
}

func (self *KiteQClient) Destory() {
	self.kclientManager.Destory()
}
