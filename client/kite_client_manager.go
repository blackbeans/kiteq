package client

import (
	"errors"
	"fmt"
	"kiteq/binding"
	"kiteq/client/chandler"
	"kiteq/client/core"
	"kiteq/client/listener"
	"kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"log"
)

const MAX_CLIENT_CONN = 10

type KiteClientManager struct {
	Subs      []*binding.Binding
	Local     string
	ZkAddr    string
	Pubs      []string
	GroupId   string
	SecretKey string
	conns     map[string]*core.KiteClientPool
	zkManager *binding.ZKManager
	listener  listener.IListener
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

func (self *KiteClientManager) newClientPool(topic string) (*core.KiteClientPool, error) {
	// @todo server地址改变的时候需要通知client重连
	addrs, err := self.zkManager.GetQServerAndWatch(topic, binding.NewWatcher(self))
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New(fmt.Sprint("%s没有可用的server地址", topic))
	}
	cpipe := pipe.NewDefaultPipeline()
	clientm := rclient.NewClientManager()
	clientm = rclient.NewClientManager()
	cpipe.RegisteHandler("kiteclient-packet", chandler.NewPacketHandler("kiteclient-packet"))
	cpipe.RegisteHandler("kiteclient-accept", chandler.NewAcceptHandler("kiteclient-accept", self.listener))
	cpipe.RegisteHandler("kiteclient-remoting", chandler.NewRemotingHandler("kiteclient-remoting", clientm))

	ins, err := core.NewKitClientPool(
		MAX_CLIENT_CONN,
		addrs,
		self.GroupId,
		self.SecretKey,
		self.Local,
		cpipe,
		func(remoteClient *rclient.RemotingClient, packet []byte) {
			event := pipe.NewPacketEvent(remoteClient, packet)
			err := cpipe.FireWork(event)
			if nil != err {
				log.Printf("KiteClient|onPacketRecieve|FAIL|%s|%t\n", err, packet)
			}
		},
	)
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

func (self *KiteClientManager) SetSubs(bindings []*binding.Binding) error {
	self.Subs = bindings
	// 注册订阅的Topics
	if err := self.zkManager.PublishBindings(
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

func (self *KiteClientManager) AddListener(listener listener.IListener) {
	self.listener = listener
}

func NewKiteClientManager(local, zkAddr, groupId, secretKey string) *KiteClientManager {
	ins := &KiteClientManager{
		Local:     local,
		ZkAddr:    zkAddr,
		GroupId:   groupId,
		SecretKey: secretKey,
		conns:     make(map[string]*core.KiteClientPool),
	}

	// 连接zookeeper
	ins.zkManager = binding.NewZKManager(ins.ZkAddr)

	return ins
}

func (self *KiteClientManager) SendStringMessage(msg *protocol.StringMessage) error {
	clientPool := self.conns[*msg.Header.Topic]
	if clientPool == nil {
		return errors.New(fmt.Sprintf("THIS CLIENT CANT SEND TYPE %s\n", msg.Header.Topic))
	}
	client := clientPool.Get()
	defer clientPool.Put(client)
	return client.SendStringMessage(msg)
}
