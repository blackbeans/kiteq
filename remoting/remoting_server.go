package remoting

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"go-kite/handler"
	"go-kite/remoting/protocol"
	"log"
	"net"
	"time"
)

type RemotingServer struct {
	hostport  string
	keepalive time.Duration
	stop      chan bool
	pipeline  *handler.DefaultPipeline
}

func NewRemotionServer(hostport string, keepalive time.Duration) *RemotingServer {
	server := &RemotingServer{
		hostport:  hostport,
		keepalive: keepalive,
		stop:      make(chan bool, 1)}
	return server
}

func (self *RemotingServer) ListenAndServer() error {
	addr, err := net.ResolveTCPAddr("ipv4", self.hostport)
	if nil != err {
		log.Fatalf("RemotingServer|ADDR|FAIL|%s\n", self.hostport)
		return err
	}

	listener, err := net.ListenTCP("ipv4", addr)
	stopListener := &StoppedListener{listener, self.stop, self.keepalive}
	//开始服务获取连接
	return self.serve(stopListener)

}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for {
		select {
		case <-self.stop:
			return l.Close()
		default:
			//do nothing
		}

		conn, err := l.AcceptTCP()
		if nil != err {

		} else {
			//session处理
			session := NewSession(conn)
			self.handleSession(session)
		}
	}
}

//处理session
func (self *RemotingServer) handleSession(session *Session) {
	//根据不同的cmdtype 走不同的processor
	go func() {
		//TODO 也许这里可以启动多个协程去读取网络字节
		for {

			//当前进程是否需要停止，如果是则关闭掉当前的session,否则去读取数据
			select {
			case <-self.stop:
				//关闭session
				session.Close()
				break
			default:
				//do nothing
			}

			//1.读取数据包
			cmdtype, packet, err := session.ReadTLV()

			//如果解析读取的数据有错误则直接略过
			if nil != err {

			} else {
				self.packetProcess(cmdtype, packet)
			}

		}
	}()
}

//数据包处理
func (self *RemotingServer) packetProcess(cmdtype uint8, packet []byte) {
	var event *handler.AcceptEvent
	var err error
	//根据类型反解packet
	switch cmdtype {
	//心跳
	case protocol.CMD_TYPE_HEARTBEAT:
		heartBeat := &protocol.HeartBeatPacket{}
		err = proto.Unmarshal(packet, heartBeat)

	case protocol.CMD_TX_ACK:
		txAck := &protocol.TranscationACKPacket{}
		err = proto.Unmarshal(packet, txAck)
	//发送的是bytesmessage
	case protocol.CMD_TYPE_BYTES_MESSAGE:
		msg := &protocol.BytesMessage{}
		err = proto.Unmarshal(packet, msg)
		if nil == err {
			event = handler.NewAcceptEvent(protocol.CMD_TYPE_BYTES_MESSAGE, msg)
		}
	//发送的是StringMessage
	case protocol.CMD_TYPE_STRING_MESSAGE:
		msg := &protocol.StringMessage{}
		err = proto.Unmarshal(packet, msg)
		if nil == err {
			event = handler.NewAcceptEvent(protocol.CMD_TYPE_STRING_MESSAGE, msg)
		}
	}

	//直接启动一个go的携程去走处理
	go func() {
		if nil == err {
			self.pipeline.FireWork(event)
		} else {
			//发序列化数据包失败

		}

	}()
}

var CONN_ERROR error = errors.New("STOP LISTENING")

//远程的listener
type StoppedListener struct {
	*net.TCPListener
	stop      chan bool
	keepalive time.Duration
}

//accept
func (self *StoppedListener) Accept() (*net.TCPConn, error) {
	for {
		conn, err := self.AcceptTCP()
		select {
		case <-self.stop:
			return nil, CONN_ERROR
		default:
			//do nothing
		}

		if nil == err {
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(self.keepalive)
		} else {
			return nil, err
		}

		return conn, err
	}
}
