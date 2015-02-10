package server

import (
	. "kiteq/remoting/client"
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
	packetDispatcher func(remoteClient *RemotingClient, packet []byte)
	flowControl      *stat.FlowControl
}

func NewRemotionServer(hostport string, keepalive time.Duration,
	flowControl *stat.FlowControl,
	packetDispatcher func(remoteClient *RemotingClient, packet []byte)) *RemotingServer {

	//设置为8个并发
	runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:         hostport,
		keepalive:        keepalive,
		stopChan:         make(chan bool, 1),
		packetDispatcher: packetDispatcher,
		isShutdown:       false,
		flowControl:      flowControl}
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
	go self.serve(stopListener)

	return nil

}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.AcceptTCP()
		if nil != err {
			log.Printf("RemotingServer|serve|AcceptTCP|FAIL|%s\n", err)
			continue
		} else {

			log.Printf("RemotingServer|serve|AcceptTCP|SUCC|%s\n", conn.RemoteAddr())
			//创建remotingClient对象
			remoteClient := NewRemotingClient(conn, self.packetDispatcher)
			remoteClient.Start()
		}
	}
	return nil
}

func (self *RemotingServer) Shutdown() {
	self.isShutdown = true
	close(self.stopChan)
	self.flowControl.Stop()
}
