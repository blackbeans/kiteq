package client

import (
	"kiteq/binding"
	"kiteq/client/core"
	"kiteq/client/listener"
	"kiteq/protocol"
)

type KiteQClient struct {
	kclientManager *core.KiteClientManager
}

func (self *KiteQClient) Start() {
	self.kclientManager.Start()
}

func NewKiteQClient(zkAddr, groupId, secretKey string, listener listener.IListener) *KiteQClient {
	return &KiteQClient{
		kclientManager: core.NewKiteClientManager(zkAddr, groupId, secretKey, listener)}
}

func (self *KiteQClient) SetTopics(topics []string) {
	self.kclientManager.SetPublishTopics(topics)
}

func (self *KiteQClient) SetBindings(bindings []*binding.Binding) {
	self.kclientManager.SetBindings(bindings)

}

func (self *KiteQClient) SendStringMessage(msg *protocol.StringMessage) error {
	return self.kclientManager.SendMessage(msg.GetHeader().GetTopic(), msg)
}

func (self *KiteQClient) SendBytesMessage(msg *protocol.BytesMessage) error {
	return self.kclientManager.SendMessage(msg.GetHeader().GetTopic(), msg)
}

func (self *KiteQClient) Destory() {
	self.kclientManager.Destory()
}
