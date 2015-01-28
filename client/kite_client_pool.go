package client

import (
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
	sync.Locker
	queue chan *KiteClient
}

func NewKitClientPool(maxConn int, remotes []string, groupId, secretKey, local string) (*KiteClientPool, error) {
	return &KiteClientPool{
		max:        maxConn,
		idle:       0,
		alloc:      0,
		roundRobin: 0,
		groupId:    groupId,
		secretKey:  secretKey,
		local:      local,
		remotes:    remotes,
		queue:      make(chan *KiteClient, maxConn),
	}, nil
}

func (self *KiteClientPool) newClient(remote string) (*KiteClient, error) {
	return NewKitClient(
		self.local,
		remote,
		self.groupId,
		self.secretKey,
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
		self.newClient(remote)
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
