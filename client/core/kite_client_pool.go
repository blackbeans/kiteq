package core

import (
	"kiteq/pipe"
	rcient "kiteq/remoting/client"
	// "log"
	"sync"
)

type KiteClientPool struct {
	idle       int
	alloc      int
	max        int
	local      string
	groupId    string
	secretKey  string
	remotes    []string
	roundRobin int
	sync.Mutex
	pipeline         *pipe.DefaultPipeline
	packetDispatcher func(remoteClient *rcient.RemotingClient, packet []byte)
	queue            chan *KiteClient
}

func NewKitClientPool(maxConn int, remotes []string, groupId, secretKey, local string,
	pipeline *pipe.DefaultPipeline,
	packetDispatcher func(remoteClient *rcient.RemotingClient, packet []byte)) (*KiteClientPool, error) {

	return &KiteClientPool{
		max:              maxConn,
		idle:             0,
		alloc:            0,
		roundRobin:       0,
		groupId:          groupId,
		secretKey:        secretKey,
		local:            local,
		remotes:          remotes,
		pipeline:         pipeline,
		packetDispatcher: packetDispatcher,
		queue:            make(chan *KiteClient, maxConn),
	}, nil
}

func (self *KiteClientPool) newClient(remote string) (*KiteClient, error) {
	return NewKitClient(
		self.groupId,
		self.secretKey,
		self.local,
		remote,
		self.packetDispatcher,
	), nil
}

func (self *KiteClientPool) Get() *KiteClient {
	self.Lock()
	defer func() {
		self.idle -= 1
		self.Unlock()
	}()

	if self.idle == 0 && self.alloc < self.max {
		// 创建一个新的链接
		remote := self.remotes[self.roundRobin%len(self.remotes)]
		client, _ := self.newClient(remote)
		self.queue <- client
		self.roundRobin += 1
		self.alloc += 1
	}
	return <-self.queue
}

func (self *KiteClientPool) Put(client *KiteClient) {
	self.Lock()
	defer func() {
		self.idle += 1
		self.Unlock()
	}()
	self.queue <- client
}
