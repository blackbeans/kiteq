package server

import (
	"go-kite/handler"
	"go-kite/remoting/session"
	"go-kite/stat"
	"log"
	"net"
	"runtime"
	"time"
)

type RemotingServer struct {
	hostport    string
	keepalive   time.Duration
	stopChan    chan bool
	isShutdown  bool
	pipeline    *handler.DefaultPipeline
	flowControl *stat.FlowControl
}

func NewRemotionServer(hostport string, keepalive time.Duration,
	pipeline *handler.DefaultPipeline) *RemotingServer {

	//设置为8个并发
	runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:    hostport,
		keepalive:   keepalive,
		stopChan:    make(chan bool, 1),
		pipeline:    pipeline,
		isShutdown:  false,
		flowControl: stat.NewFlowControl(hostport)}
	return server
}

func (self *RemotingServer) ListenAndServer() error {
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
			session := session.NewSession(conn, self.hostport, self.onPacketRecieve, self.flowControl)
			self.handleSession(session)
		}
	}
	return nil
}

//处理session
func (self *RemotingServer) handleSession(session *session.Session) {
	//根据不同的cmdtype 走不同的processor
	//读取合法的包
	go session.ReadPacket()
	//分发包
	go session.DispatcherPacket()
	//开启网路的写出packet
	go session.WritePacket()

}

//数据包处理
func (self *RemotingServer) onPacketRecieve(session *session.Session, packet []byte) {

	event := handler.NewPacketEvent(session, packet)
	err := self.pipeline.FireWork(event)
	if nil != err {
		log.Printf("RemotingServer|onPacketRecieve|FAIL|%s|%t\n", err, packet)
	}
}

func (self *RemotingServer) Shutdown() {
	self.isShutdown = true
	close(self.stopChan)
	self.flowControl.Stop()
}
