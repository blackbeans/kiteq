package client

import (
	"errors"
	"fmt"
	log "github.com/blackbeans/log4go"
	"kiteq/protocol"
	"kiteq/remoting/session"
	"net"
	"time"
)

//网络层的client
type RemotingClient struct {
	conn             *net.TCPConn
	localAddr        string
	remoteAddr       string
	heartbeat        int64
	remoteSession    *session.Session
	packetDispatcher func(remoteClient *RemotingClient, packet *protocol.Packet) //包处理函数
	rc               *protocol.RemotingConfig
}

func NewRemotingClient(conn *net.TCPConn,
	packetDispatcher func(remoteClient *RemotingClient, packet *protocol.Packet),
	rc *protocol.RemotingConfig) *RemotingClient {

	remoteSession := session.NewSession(conn, rc)

	//创建一个remotingcleint
	remotingClient := &RemotingClient{
		heartbeat:        0,
		conn:             conn,
		packetDispatcher: packetDispatcher,
		remoteSession:    remoteSession,
		rc:               rc}

	return remotingClient
}

func (self *RemotingClient) RemoteAddr() string {
	return self.remoteAddr
}

func (self *RemotingClient) LocalAddr() string {
	return self.localAddr
}

func (self *RemotingClient) Idle() bool {
	return self.remoteSession.Idle()
}

//启动当前的client
func (self *RemotingClient) Start() {

	//重新初始化
	laddr := self.conn.LocalAddr().(*net.TCPAddr)
	raddr := self.conn.RemoteAddr().(*net.TCPAddr)
	self.localAddr = fmt.Sprintf("%s:%d", laddr.IP, laddr.Port)
	self.remoteAddr = fmt.Sprintf("%s:%d", raddr.IP, raddr.Port)

	//开启写操作
	go self.remoteSession.WritePacket()

	//开启转发
	go self.dispatcherPacket(self.remoteSession)

	//启动读取
	go self.remoteSession.ReadPacket()

	log.Info("RemotingClient|Start|SUCC|local:%s|remote:%s\n", self.LocalAddr(), self.RemoteAddr())
}

//重连
func (self *RemotingClient) reconnect() (bool, error) {

	conn, err := net.DialTCP("tcp4", nil, self.conn.RemoteAddr().(*net.TCPAddr))
	if nil != err {
		log.Error("RemotingClient|RECONNECT|%s|FAIL|%s\n", self.RemoteAddr(), err)
		return false, err
	}

	//重新设置conn
	self.conn = conn
	//创建session
	self.remoteSession = session.NewSession(self.conn, self.rc)

	//再次启动remoteClient
	self.Start()
	return true, nil
}

//包分发
func (self *RemotingClient) dispatcherPacket(session *session.Session) {

	//解析包
	for nil != self.remoteSession &&
		!self.remoteSession.Closed() {

		packet := <-self.remoteSession.ReadChannel
		//获取协程处理分发包
		self.rc.MaxDispatcherNum <- 1
		self.rc.FlowStat.DispatcherWorkPool.Incr(1)
		go func() {
			defer func() {
				<-self.rc.MaxDispatcherNum
				self.rc.FlowStat.DispatcherWorkPool.Incr(-1)
			}()
			//处理一下包
			self.packetDispatcher(self, &packet)

		}()

		if nil != self.rc.FlowStat {
			self.rc.FlowStat.DispatcherFlow.Incr(1)
		}
	}

}

var ERROR_PONG = errors.New("ERROR PONG TYPE !")

//同步发起ping的命令
func (self *RemotingClient) Ping(heartbeat *protocol.Packet, timeout time.Duration) error {
	pong, err := self.WriteAndGet(*heartbeat, timeout)
	if nil != err {
		return err
	}
	version, ok := pong.(int64)
	if !ok {
		log.Warn("RemotingClient|Ping|Pong|ERROR TYPE |%s\n", pong)
		return ERROR_PONG
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
		id := self.rc.RequestHolder.CurrentOpaque()
		packet.Opaque = id
		tid = id
	}

	return tid, make(chan interface{}, 1)
}

//将结果attach到当前的等待回调chan
func (self *RemotingClient) Attach(opaque int32, obj interface{}) {
	defer func() {
		if err := recover(); nil != err {
			log.Error("RemotingClient|Attach|FAIL|%s|%s\n", err, obj)
		}
	}()

	self.rc.RequestHolder.Detach(opaque, obj)

}

//只是写出去
func (self *RemotingClient) Write(packet protocol.Packet) (chan interface{}, error) {

	opaque, future := self.fillOpaque(&packet)
	self.rc.RequestHolder.Attach(opaque, future)
	return future, self.remoteSession.Write(packet)

}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

//写数据并且得到相应
func (self *RemotingClient) WriteAndGet(packet protocol.Packet,
	timeout time.Duration) (interface{}, error) {

	//同步写出
	future, err := self.Write(packet)
	if nil != err {
		return nil, err
	}
	var resp interface{}
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
	log.Info("RemotingClient|Shutdown|%s...", self.LocalAddr())
}
