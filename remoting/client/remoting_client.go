package client

import (
	"errors"
	"fmt"
	"kiteq/protocol"
	"kiteq/remoting/session"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_WATER_MARK int = 100000
)

//网络层的client
type RemotingClient struct {
	id               int32
	remoteAddr       *net.TCPAddr
	heartbeat        int64
	remoteSession    *session.Session
	packetDispatcher func(remoteClient *RemotingClient, packet []byte) //包处理函数
	lock             sync.Mutex
	holder           map[int32]chan interface{}
}

func NewRemotingClient(conn *net.TCPConn,
	packetDispatcher func(remoteClient *RemotingClient, packet []byte)) *RemotingClient {

	remoteSession := session.NewSession(conn)
	//创建一个remotingcleint
	remotingClient := &RemotingClient{
		id:               0,
		heartbeat:        0,
		remoteAddr:       conn.RemoteAddr().(*net.TCPAddr),
		packetDispatcher: packetDispatcher,
		remoteSession:    remoteSession,
		holder:           make(map[int32]chan interface{})}

	return remotingClient
}

func (self *RemotingClient) RemoteAddr() string {
	return fmt.Sprintf("%s:%d", self.remoteAddr.IP, self.remoteAddr.Port)
}

//启动当前的client
func (self *RemotingClient) Start() {

	for i := 0; i < 20; i++ {
		//开启写操作
		go self.remoteSession.WritePacket()
	}

	//开启转发
	for i := 0; i < 50; i++ {

		go self.dispatcherPacket(self.remoteSession)
	}

	//启动读取
	go self.remoteSession.ReadPacket()

	log.Printf("RemotingClient|Start|SUCC|%s\n", self.RemoteAddr())
}

//重连
func (self *RemotingClient) reconnect() (bool, error) {
	conn, err := net.DialTCP("tcp4", nil, self.remoteAddr)
	if nil != err {
		log.Printf("RemotingClient|RECONNECT|%s|FAIL|%s\n", self.RemoteAddr(), err)
		return false, err
	}

	if nil != err {
		return false, err
	}

	self.remoteSession = session.NewSession(conn)
	if nil != err {
		log.Printf("RemotingClient|RECONNECT|FAIL|%s\n", err)
		return false, err
	}
	//再次启动remoteClient
	self.Start()
	return true, nil
}

//包分发
func (self *RemotingClient) dispatcherPacket(session *session.Session) {

	//解析包
	for nil != self.remoteSession &&
		!self.remoteSession.Closed() {
		select {
		//100ms读超时
		case <-time.After(100 * time.Millisecond):
		//1.读取数据包
		case packet := <-self.remoteSession.ReadChannel:
			//2.处理一下包
			go self.packetDispatcher(self, packet)

		}
	}

}

//同步发起ping的命令
func (self *RemotingClient) Ping(heartbeat *protocol.Packet, timeout time.Duration) error {
	pong, err := self.WriteAndGet(heartbeat, timeout)
	if nil != err {
		return err
	}
	version, ok := pong.(int64)
	if !ok {

		return errors.New(fmt.Sprintf("ERROR PONG TYPE !|%t", pong))
	}
	self.updateHeartBeat(version)
	return nil
}

func (self *RemotingClient) updateHeartBeat(version int64) {
	if version > self.heartbeat {
		self.heartbeat = version
	}
}

func (self *RemotingClient) Pong(opaque int32, version int64) {
	self.updateHeartBeat(version)
}

func (self *RemotingClient) fillOpaque(packet *protocol.Packet) (int32, chan interface{}) {
	tid := packet.Opaque
	//只有在默认值没有赋值的时候才去赋值
	if tid < 0 {
		id := atomic.AddInt32(&self.id, 1) % int32(MAX_WATER_MARK)
		packet.Opaque = id
		tid = id
	}
	return tid, make(chan interface{}, 1)
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
	tid, future := self.fillOpaque(packet)
	self.lock.Lock()
	old, ok := self.holder[tid]
	if ok {
		delete(self.holder, tid)
		close(old)
	}
	self.holder[tid] = future
	self.lock.Unlock()

	self.remoteSession.WriteChannel <- packet.Marshal()
	return future
}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

//写数据并且得到相应
func (self *RemotingClient) WriteAndGet(packet *protocol.Packet,
	timeout time.Duration) (interface{}, error) {

	future := self.Write(packet)

	var resp interface{}
	//
	select {
	case <-time.After(timeout):
		//删除掉当前holder
		return nil, TIMEOUT_ERROR
	case resp = <-future:
		return resp, nil
	}

}

func (self *RemotingClient) IsClosed() bool {
	return self.remoteSession.Closed()
}

func (self *RemotingClient) Shutdown() {
	self.remoteSession.Close()
}
