package session

import (
	"bufio"
	"errors"
	"fmt"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"github.com/blackbeans/turbo/codec"
	"github.com/blackbeans/turbo/packet"
	"io"
	"math"
	"net"
	"time"
)

type Session struct {
	conn         *net.TCPConn //tcp的session
	remoteAddr   string
	br           *bufio.Reader
	bw           *bufio.Writer
	ReadChannel  chan *packet.Packet //request的channel
	WriteChannel chan *packet.Packet //response的channel
	isClose      bool
	lasttime     time.Time
	rc           *turbo.RemotingConfig
	frameCodec   codec.ICodec
}

func NewSession(conn *net.TCPConn, rc *turbo.RemotingConfig,
	frameCodec codec.ICodec) *Session {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(rc.IdleTime * 2)
	//禁用nagle
	conn.SetNoDelay(true)
	conn.SetReadBuffer(rc.ReadBufferSize)
	conn.SetWriteBuffer(rc.WriteBufferSize)

	session := &Session{
		conn:         conn,
		br:           bufio.NewReaderSize(conn, rc.ReadBufferSize),
		bw:           bufio.NewWriterSize(conn, rc.WriteBufferSize),
		ReadChannel:  make(chan *packet.Packet, rc.ReadChannelSize),
		WriteChannel: make(chan *packet.Packet, rc.WriteChannelSize),
		isClose:      false,
		remoteAddr:   conn.RemoteAddr().String(),
		frameCodec:   frameCodec,
		rc:           rc}
	//连接数计数
	rc.FlowStat.Connections.Incr(1)
	return session
}

func (self *Session) RemotingAddr() string {
	return self.remoteAddr
}

func (self *Session) Idle() bool {
	//当前时间如果大于 最后一次发包时间+2倍的idletime 则认为空心啊
	return time.Now().After(self.lasttime.Add(self.rc.IdleTime))
}

//读取
func (self *Session) ReadPacket() {

	//缓存本次包的数据
	for !self.isClose {

		func() {
			defer func() {
				if err := recover(); nil != err {
					log.Error("Session|ReadPacket|%s|recover|FAIL|%s", self.remoteAddr, err)
				}
			}()
			buffer, err := self.frameCodec.Read(self.br)
			if nil != err {
				self.Close()
				log.Error("Session|ReadPacket|%s|FAIL|CLOSE SESSION|%s", self.remoteAddr, err)
				return
			} else {
				// log.Debug("Session|ReadPacket|%s|SUCC|%d", self.remoteAddr, buffer.Len())
			}
			bl := buffer.Len()
			p, err := self.frameCodec.UnmarshalPacket(buffer)
			if nil != err {
				self.Close()
				log.Error("Session|ReadPacket|MarshalPacket|%s|FAIL|CLOSE SESSION|%s", self.remoteAddr, err)
				return
			}
			// fmt.Println("ReadPacket|" + self.RemotingAddr() + "\t" + string(p.Data))
			//写入缓冲
			self.ReadChannel <- p
			//重置buffer
			if nil != self.rc.FlowStat {
				self.rc.FlowStat.ReadFlow.Incr(1)
				self.rc.FlowStat.ReadBytesFlow.Incr(int32(bl))
			}
		}()
	}
}

//写出数据
func (self *Session) Write(p *packet.Packet) error {
	defer func() {
		if err := recover(); nil != err {
			log.Error("Session|Write|%s|recover|FAIL|%s", self.remoteAddr, err)
		}
	}()

	if !self.isClose {
		select {
		case self.WriteChannel <- p:
			return nil
		default:
			return errors.New(fmt.Sprintf("WRITE CHANNLE [%s] FULL", self.remoteAddr))
		}
	}
	return errors.New(fmt.Sprintf("Session|[%s]|CLOSED", self.remoteAddr))
}

//真正写入网络的流
func (self *Session) write0(tlv []*packet.Packet) {
	batch := make([]byte, 0, len(tlv)*128)
	for _, t := range tlv {
		p := self.frameCodec.MarshalPacket(t)
		if nil == p || len(p) <= 0 {
			log.Error("Session|write0|MarshalPacket|FAIL|EMPTY PACKET|%s", t)
			//如果是同步写出
			continue
		}
		batch = append(batch, p...)
	}

	if len(batch) <= 0 {
		return
	}

	l := 0
	tmp := batch
	for {
		length, err := self.bw.Write(tmp)
		if nil != err {
			log.Error("Session|write0|conn|%s|FAIL|%s|%d/%d", self.remoteAddr, err, length, len(tmp))
			//链接是关闭的
			if err != io.ErrShortWrite {
				self.Close()
				return
			}

			//如果没有写够则再写一次
			if err == io.ErrShortWrite {
				self.bw.Reset(self.conn)
			}
		}

		l += length
		//write finish
		if l == len(batch) {
			break
		}
		tmp = batch[l:]
	}
	// //flush
	self.bw.Flush()
	if nil != self.rc.FlowStat {
		self.rc.FlowStat.WriteFlow.Incr(1)
		self.rc.FlowStat.WriteBytesFlow.Incr(int32(len(batch)))
	}

}

//写入响应
func (self *Session) WritePacket() {
	packets := make([]*packet.Packet, 0, 100)
	for !self.isClose {

		p := <-self.WriteChannel
		if nil != p {
			packets = append(packets, p)
		}
		l := int(math.Min(float64(len(self.WriteChannel)), 100))
		//如果channel的长度还有数据批量最多读取100合并写出
		//减少系统调用
		for i := 0; i < l; i++ {
			p := <-self.WriteChannel
			if nil != p {
				packets = append(packets, p)
			}
		}

		if len(packets) > 0 {
			//批量写入
			self.write0(packets)
			self.lasttime = time.Now()
			packets = packets[:0]
		}

	}

	//deal left packet
	for {
		_, ok := <-self.WriteChannel
		if !ok {
			//channel closed
			break
		}
	}
}

//当前连接是否关闭
func (self *Session) Closed() bool {
	return self.isClose
}

func (self *Session) Close() error {

	if !self.isClose {
		self.isClose = true
		//flush
		self.bw.Flush()
		self.conn.Close()
		close(self.WriteChannel)
		close(self.ReadChannel)
		self.rc.FlowStat.Connections.Incr(-1)
		log.Debug("Sessio`n|Close|%s...", self.remoteAddr)
	}

	return nil
}
