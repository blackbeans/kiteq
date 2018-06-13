package turbo

import (
	log "github.com/blackbeans/log4go"
	"net"
	"time"
	"runtime"
)

type TServer struct {
	hostport   string
	keepalive  time.Duration
	stopChan   chan bool
	isShutdown bool
	onMessage  THandler
	config     *TConfig
	codec      func() ICodec
}

func NewTServer(hostport string, config *TConfig,
	onMessage THandler) *TServer {

	runtime.GOMAXPROCS(runtime.NumCPU() + 1)

	server := &TServer{
		hostport:   hostport,
		stopChan:   make(chan bool, 1),
		onMessage:  onMessage,
		isShutdown: false,
		config:     config,
		keepalive:  5 * time.Minute,
		codec: func() ICodec {
			return LengthBytesCodec{
				MaxFrameLength: MAX_PACKET_BYTES}

		}}
	return server
}

func NewTServerWithCodec(hostport string, config *TConfig, codec func() ICodec,
	onMessage THandler) *TServer {

	//设置为8个并发
	// runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &TServer{
		hostport:   hostport,
		stopChan:   make(chan bool, 1),
		onMessage:  onMessage,
		isShutdown: false,
		config:     config,
		keepalive:  5 * time.Minute,
		codec:      codec}
	return server
}

func (self *TServer) ListenAndServer() error {

	addr, err := net.ResolveTCPAddr("tcp4", self.hostport)
	if nil != err {
		log.Error("TServer|ADDR|FAIL|%s\n", self.hostport)
		return err
	}

	listener, err := net.ListenTCP("tcp4", addr)
	if nil != err {
		log.Error("TServer|ListenTCP|FAIL|%v|%s",err, addr)
		return err
	}


	stopListener := &StoppedListener{listener, self.stopChan, self.keepalive}

	//开始服务获取连接
	go self.serve(stopListener)
	return nil

}

//networkstat
func (self *TServer) NetworkStat() NetworkStat {
	return self.config.FlowStat.Stat()

}

func (self *TServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.Accept()
		if nil != err {
			log.Error("TServer|serve|AcceptTCP|FAIL|%s\n", err)
			continue
		} else {
			// log.Debug("TServer|serve|AcceptTCP|SUCC|%s\n", conn.RemoteAddr())
			//创建remotingClient对象

			remoteClient := NewTClient(conn, self.codec,self.onMessage, self.config)
			remoteClient.Start()
		}
	}
	return nil
}

func (self *TServer) Shutdown() {
	self.isShutdown = true
	close(self.stopChan)
	log.Info("TServer|Shutdown...")
}
