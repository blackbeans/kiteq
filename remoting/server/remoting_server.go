package server

import (
	rclient "kiteq/remoting/client"
	"kiteq/remoting/session"
	"kiteq/stat"
	"log"
	"net"
	"runtime"
	"time"
)

type RemotingServer struct {
	hostport         string
	keepalive        time.Duration
	stopChan         chan bool
	isShutdown       bool
	packetDispatcher func(remoteClient *rclient.RemotingClient, packet []byte)
	flowControl      *stat.FlowControl
}

func NewRemotionServer(hostport string, keepalive time.Duration,
	packetDispatcher func(remoteClient *rclient.RemotingClient, packet []byte)) *RemotingServer {

	//设置为8个并发
	runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:         hostport,
		keepalive:        keepalive,
		stopChan:         make(chan bool, 1),
		packetDispatcher: packetDispatcher,
		isShutdown:       false,
		flowControl:      stat.NewFlowControl(hostport)}
	return server
}

func (self *RemotingServer) ListenAndServer() error {

	//开启流控

	self.flowControl.Start()
	addr, err := net.ResolveTCPAddr("tcp4", self.hostport)
	if nil != err {
		log.Fatalf("RemotingServer|ADDR|FAIL|%s\n", self.hostport)
		return err
	}

	listener, err := net.ListenTCP("tcp4", addr)
	if nil != err {
		log.Fatalf("RemotingServer|ListenTCP|FAIL|%s\n", addr)
		return err
	}

	stopListener := &StoppedListener{listener, self.stopChan, self.keepalive}

	//开始服务获取连接
	return self.serve(stopListener)

}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.AcceptTCP()
		if nil != err {
			log.Printf("RemotingServer|serve|AcceptTCP|FAIL|%s\n", err)
			return err
		} else {

			log.Printf("RemotingServer|serve|AcceptTCP|SUCC|%s\n", conn.RemoteAddr())
			//session处理,应该有个session管理器
			rempteSession := session.NewSession(conn, self.hostport, self.flowControl)
			self.handleSession(rempteSession)
		}
	}
	return nil
}

//处理session
func (self *RemotingServer) handleSession(session *session.Session) {
	//根据不同的cmdtype 走不同的packetDispatcheror
	//读取合法的包
	remoteClient := rclient.NewRemotingClient(session, self.packetDispatcher)
	remoteClient.Start()

}

func (self *RemotingServer) Shutdown() {
	self.isShutdown = true
	close(self.stopChan)
	self.flowControl.Stop()
}
