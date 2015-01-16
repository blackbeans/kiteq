package server

import (
	"go-kite/handler"
	"go-kite/remoting/session"
	"log"
	"net"
	"time"
)

type RemotingServer struct {
	hostport   string
	addr       *net.TCPAddr
	keepalive  time.Duration
	stopChan   chan bool
	isShutdown bool
	pipeline   *handler.DefaultPipeline
}

func NewRemotionServer(hostport string, keepalive time.Duration,
	pipeline *handler.DefaultPipeline) *RemotingServer {
	server := &RemotingServer{
		hostport:   hostport,
		keepalive:  keepalive,
		stopChan:   make(chan bool, 1),
		pipeline:   pipeline,
		isShutdown: false}
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

	self.addr = addr

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
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(self.keepalive)
			conn.SetNoDelay(true)

			//session处理,应该有个session管理器
			session := session.NewSession(conn, self.addr)
			self.handleSession(session)
		}
	}
	return nil
}

//处理session
func (self *RemotingServer) handleSession(session *session.Session) {
	//根据不同的cmdtype 走不同的processor

	go func() {
		//读取合法的包
		go session.ReadPacket()
		//解析包
		for !self.isShutdown {

			//1.读取数据包
			packet := <-session.RequestChannel

			//处理一下包
			go self.onPacketRecieve(session, packet)
		}
	}()
}

//数据包处理
func (self *RemotingServer) onPacketRecieve(session *session.Session, packet []byte) {

	event := handler.NewPacketEvent(session, packet)
	self.pipeline.FireWork(event)
}

func (self *RemotingServer) Shutdonw() {
	self.isShutdown = true
	close(self.stopChan)
}
