package client

import (
	"errors"
	"kiteq/protocol"
	"kiteq/remoting/session"
	// "log"
	"sync"
	"sync/atomic"
	"time"
)

var MAX_WATER_MARK int = 100000

//网络层的client
type RemotingClient struct {
	id               int32
	isClose          bool
	remoteSession    *session.Session
	packetDispatcher func(remoteClient *RemotingClient, packet []byte) //包处理函数
	lock             sync.Mutex
	holder           map[int32]chan interface{}
}

func NewRemotingClient(remoteSession *session.Session,
	packetDispatcher func(remoteClient *RemotingClient, packet []byte)) *RemotingClient {

	//创建一个remotingcleint
	remotingClient := &RemotingClient{
		isClose:          false,
		packetDispatcher: packetDispatcher,
		remoteSession:    remoteSession,
		holder:           make(map[int32]chan interface{})}

	return remotingClient
}

func (self *RemotingClient) RemoteAddr() string {
	return self.remoteSession.RemotingAddr()
}

func (self *RemotingClient) Start() {

	//开启写操作
	go self.remoteSession.WritePacket()

	//开启转发
	go self.dispatcherPacket(self.remoteSession)

	//启动读取
	go self.remoteSession.ReadPacket()
}

func (self *RemotingClient) dispatcherPacket(session *session.Session) {

	//50个读协程
	for i := 0; i < 50; i++ {
		go func() {
			//解析包
			for !self.isClose {
				select {
				//1.读取数据包
				case packet := <-session.ReadChannel:
					//2.处理一下包
					go self.packetDispatcher(self, packet)
					//100ms读超时
				case <-time.After(100 * time.Millisecond):
				}

			}
		}()
	}
}

var TIMEOUT_ERROR = errors.New("SEND MESSAGE TIMEOUT ")

func (self *RemotingClient) fillOpaque(packet *protocol.Packet) int32 {
	tid := packet.Opaque
	//只有在默认值没有赋值的时候才去赋值
	if tid < 0 {
		id := atomic.AddInt32(&self.id, 1) % int32(MAX_WATER_MARK)
		packet.Opaque = id
		tid = id
	}
	return tid
}

//将结果attach到当前的等待回调chan
func (self *RemotingClient) Attach(opaque int32, obj interface{}) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ch, ok := self.holder[opaque]
	if ok {
		ch <- obj
		delete(self.holder, opaque)
		close(ch)
	}
}

//只是写出去
func (self *RemotingClient) Write(packet *protocol.Packet) chan interface{} {
	tid := self.fillOpaque(packet)
	self.lock.Lock()
	self.holder[tid] = packet.Get()
	self.lock.Unlock()

	self.remoteSession.WriteChannel <- packet.Marshal()
	return packet.Get()
}

//写数据并且得到相应
func (self *RemotingClient) WriteAndGet(packet *protocol.Packet,
	timeout time.Duration) (interface{}, error) {

	tid := self.fillOpaque(packet)

	self.lock.Lock()
	self.holder[tid] = packet.Get()
	self.lock.Unlock()

	self.remoteSession.WriteChannel <- packet.Marshal()

	var resp interface{}
	//
	select {
	case <-time.After(timeout):
		return nil, TIMEOUT_ERROR
	case resp = <-packet.Get():
		return resp, nil
	}
}

func (self *RemotingClient) Shutdown() {
	self.isClose = true
	self.remoteSession.Close()
}
