package client

import (
	"errors"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"kiteq/binding"
	"kiteq/protocol"
	"log"
	"os"
)

const MAX_CLIENT_CONN = 10

type KiteClientManager struct {
	Subs      []*binding.Binding `toml:"subs"`
	Local     string             `toml:local`
	ZkAddr    string             `toml:zkAddr`
	Pubs      []string           `toml:"pubs"`
	GroupId   string             `toml:"groupId"`
	SecretKey string             `toml:"secretKey"`
	pubConns  map[string]*KiteClientPool
	subConns  map[string]*KiteClientPool
	zkManager *binding.ZKManager
}

func (self *KiteClientManager) String() string {
	return fmt.Sprintf("\npubs:%s \nlocal:%s \nsubs:%s\n", self.Pubs, self.Local, self.Subs)
}

func (self *KiteClientManager) EventNotify(path string, eventType binding.ZkEvent) {
	log.Println("KITE CLIENT MANAGER|ZKEVENT NOTIFY|PATH|%s|ZKEVENT|%s\n", path, eventType)
}

func (self *KiteClientManager) ChildWatcher(path string, childNode []string) {
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

func (self *KiteClientManager) zkRegister() error {
	// 注册发送的Topics
	if err := self.zkManager.PublishTopic(
		self.Pubs, self.GroupId, self.Local); err != nil {
		return err
	}

	// 注册订阅的Topics
	if err := self.zkManager.SubscribeTopic(
		self.GroupId, self.Subs); err != nil {
		return err
	}

	// 创建publish的KiteClient
	for _, topic := range self.Pubs {
		client, err := self.newClientPool(topic)
		if err != nil {
			return err
		}
		self.pubConns[topic] = client
	}

	// 创建subscribe的KiteClient
	for _, sub := range self.Subs {
		client, err := self.newClientPool(sub.Topic)
		if err != nil {
			return err
		}
		self.subConns[sub.Topic] = client
	}

	return nil
}

func NewKiteClientManager() *KiteClientManager {
	ins := &KiteClientManager{
		pubConns: make(map[string]*KiteClientPool),
		subConns: make(map[string]*KiteClientPool),
	}
	configFile := flag.String("conf", "conf.toml", "TOML configuration file")
	flag.Parse()
	f, err := os.Open(*configFile)
	if err != nil {
		log.Fatalf("KITECLIENT|NEWCONF|FAIL|%s|%s\n", *configFile, err)
	}
	_, err = toml.DecodeReader(f, ins)

	if err != nil {
		log.Fatal("KITECLIENT|NEWCONF|FAIL|%s|%s\n", *configFile, err)
	}
	// 连接zookeeper
	ins.zkManager = binding.NewZKManager(ins.ZkAddr)

	// 将自己的订阅和发布信息注册到zk
	if err := ins.zkRegister(); err != nil {
		log.Fatalf("KITECLIENT|ZKREGISTER|FAIL|%s\n", err)
	}
	return ins
}

func (self *KiteClientManager) SendMessage(msg *protocol.StringMessage) error {
	clientPool := self.pubConns[*msg.Header.Topic]
	if clientPool == nil {
		return errors.New(fmt.Sprintf("THIS CLIENT CANT SEND TYPE %s\n", msg.Header.Topic))
	}
	client := clientPool.Get()
	return client.SendMessage(msg)
}
