package client

import (
	"kiteq/handler"
	"kiteq/protocol"
	"kiteq/remoting/session"
	"kiteq/stat"
	"log"
	"net"
	"time"
)

//网络层的client
type RemotingClient struct {
	remote      string
	local       string
	isClose     bool
	pipeline    *handler.DefaultPipeline
	keepalive   time.Duration
	session     *session.Session
	flowControl *stat.FlowControl
}

func NewRemotingClient(local, remote string, flowControl *stat.FlowControl, keepalive time.Duration,
	pipeline *handler.DefaultPipeline) *RemotingClient {

	//创建一个remotingcleint
	remotingClient := &RemotingClient{
		remote:      remote,
		local:       local,
		isClose:     false,
		keepalive:   keepalive,
		pipeline:    pipeline,
		flowControl: flowControl}

	remotingClient.Start()
	return remotingClient
}

func (self *RemotingClient) Start() {

	//连接
	conn, err := self.dial()
	if nil != err {
		log.Fatalf("RemotingClient|START|FAIL|%s\n", err)
	} else {
		self.session = session.NewSession(conn, self.remote, self.onPacketRecieve, self.flowControl)
	}

	//开启流控
	self.flowControl.Start()

	//开启写操作
	go self.session.WritePacket()

	go self.session.DispatcherPacket()
	//启动读取
	go self.session.ReadPacket()

	// //握手完成
	// err = self.handShake()
	// if nil != err {
	// 	log.Fatalf("RemotingClient|START|FAIL|%s\n", err)
	// 	return
	// }
}

func (self *RemotingClient) dial() (*net.TCPConn, error) {
	localAddr, err_l := net.ResolveTCPAddr("tcp4", self.local)
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", self.remote)
	if nil != err_l || nil != err_r {
		log.Fatalf("RemotingClient|RESOLVE ADDR |FAIL|L:%s|R:%s", err_l, err_r)
		return nil, err_l
	}
	conn, err := net.DialTCP("tcp4", localAddr, remoteAddr)
	if nil != err {
		log.Fatalf("RemotingClient|CONNECT|%s|FAIL|%s\n", self.remote, err)
		return nil, err
	}

	return conn, nil
}

//数据包处理
func (self *RemotingClient) onPacketRecieve(session *session.Session, packet []byte) {

	event := handler.NewPacketEvent(session, packet)
	err := self.pipeline.FireWork(event)
	if nil != err {
		log.Printf("RemotingClient|onPacketRecieve|FAIL|%s|%t\n", err, packet)
	}
}

//处理session
func (self *RemotingClient) handleSession(session *session.Session) {
	//根据不同的cmdtype 走不同的processor
	//读取合法的包
	go session.ReadPacket()
	//分发包
	go session.DispatcherPacket()
	//开启网路的写出packet
	go session.WritePacket()

}

//写数据
func (self *RemotingClient) Write(packet protocol.ITLVPacket) {
	self.session.WriteChannel <- packet.Marshal()
}

func (self *RemotingClient) Shutdown() {
	self.isClose = true
	self.session.Close()
	self.flowControl.Stop()
}
