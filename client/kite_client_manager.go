package client

import (
	"errors"
	"fmt"
	"kiteq/binding"
	"kiteq/protocol"
	"log"
)

const MAX_CLIENT_CONN = 10

type SubscribeCallback func(msg *protocol.StringMessage) bool
type PublishCallback func(messageId string) bool

type KiteClientManager struct {
	Subs      []*binding.Binding
	Local     string
	ZkAddr    string
	Pubs      []string
	GroupId   string
	SecretKey string
	conns     map[string]*KiteClientPool
	zkManager *binding.ZKManager
}

func (self *KiteClientManager) String() string {
	return fmt.Sprintf("\npubs:%s \nlocal:%s \nsubs:%s\n", self.Pubs, self.Local, self.Subs)
}

func (self *KiteClientManager) EventNotify(path string, eventType binding.ZkEvent, binds []*binding.Binding) {
	// @todo关闭或者新增相应的pub/sub connections
	log.Println("KITE CLIENT MANAGER|ZKEVENT NOTIFY|PATH|%s|ZKEVENT|%s\n", path, eventType)
}

func (self *KiteClientManager) ChildWatcher(path string, childNode []string) {
	// @todo关闭或者新增相应的pub/sub connections
	log.Println("KITE CLIENT MANAGER|ZK CHILDWATCHER|PATH|%s|CHILDREN|%s\n", path, childNode)
}

func (self *KiteClientManager) newClientPool(topic string) (*KiteClientPool, error) {
	// @todo server地址改变的时候需要通知client重连
	addrs, err := self.zkManager.GetQServerAndWatch(topic, binding.NewWatcher(self))
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New(fmt.Sprint("%s没有可用的server地址", topic))
	}
	ins, err := NewKitClientPool(MAX_CLIENT_CONN, addrs, self.GroupId, self.SecretKey, self.Local)
	if err != nil {
		return nil, err
	}
	return ins, nil
}

func (self *KiteClientManager) SetPubs(topics []string) error {
	self.Pubs = topics
	// 注册发送的Topics
	if err := self.zkManager.PublishTopic(
		self.Pubs, self.GroupId, self.Local); err != nil {
		return err
	}

	// 创建publish的KiteClient
	for _, topic := range self.Pubs {
		if self.conns[topic] == nil {
			client, err := self.newClientPool(topic)
			if err != nil {
				return err
			}
			self.conns[topic] = client
		}
	}

	return nil
}

func (self *KiteClientManager) SetSubs(bindings []*binding.Binding, callback SubscribeCallback) error {
	self.Subs = bindings
	// 注册订阅的Topics
	if err := self.zkManager.SubscribeTopic(
		self.GroupId, self.Subs); err != nil {
		return err
	}

	// 创建subscribe的KiteClient
	for _, sub := range self.Subs {
		if self.conns[sub.Topic] == nil {
			client, err := self.newClientPool(sub.Topic)
			if err != nil {
				return err
			}
			self.conns[sub.Topic] = client
		}
	}

	return nil
}

func NewKiteClientManager(local, zkAddr, groupId, secretKey string) *KiteClientManager {
	ins := &KiteClientManager{
		Local:     local,
		ZkAddr:    zkAddr,
		GroupId:   groupId,
		SecretKey: secretKey,
		conns:     make(map[string]*KiteClientPool),
	}

	// 连接zookeeper
	ins.zkManager = binding.NewZKManager(ins.ZkAddr)

	return ins
}

func (self *KiteClientManager) SendMessage(msg *protocol.StringMessage) error {
	log.Println("send ", *msg.Header.Topic)
	clientPool := self.conns[*msg.Header.Topic]
	if clientPool == nil {
		return errors.New(fmt.Sprintf("THIS CLIENT CANT SEND TYPE %s\n", msg.Header.Topic))
	}
	client := clientPool.Get()
	defer clientPool.Put(client)
	return client.SendStringMessage(msg)
}
