package session

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"kiteq/protocol"
	"log"
	"net"
	"syscall"
	"time"
)

type Session struct {
	conn         *net.TCPConn //tcp的session
	remoteAddr   string
	br           *bufio.Reader
	bw           *bufio.Writer
	idleTimer    *time.Timer          //空闲timer
	ReadChannel  chan protocol.Packet //request的channel
	WriteChannel chan protocol.Packet //response的channel
	isClose      bool
	rc           *protocol.RemotingConfig
}

func NewSession(conn *net.TCPConn, rc *protocol.RemotingConfig) *Session {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Second)
	//禁用nagle
	conn.SetNoDelay(true)
	conn.SetReadBuffer(rc.ReadBufferSize)
	conn.SetWriteBuffer(rc.WriteBufferSize)

	session := &Session{
		conn:         conn,
		br:           bufio.NewReaderSize(conn, rc.ReadBufferSize),
		bw:           bufio.NewWriterSize(conn, rc.WriteBufferSize),
		ReadChannel:  make(chan protocol.Packet, rc.ReadChannelSize),
		WriteChannel: make(chan protocol.Packet, rc.WriteChannelSize),
		isClose:      false,
		remoteAddr:   conn.RemoteAddr().String(),
		rc:           rc,
		idleTimer:    time.NewTimer(rc.IdleTime)}
	return session
}

func (self *Session) RemotingAddr() string {
	return self.remoteAddr
}

func (self *Session) Idle() bool {
	select {
	case <-self.idleTimer.C:
		//超时，暂时IO处于空闲
		return true
	default:
		//没有超时则是一直有读写
		return false
	}
}

//读取
func (self *Session) ReadPacket() {

	defer func() {
		if err := recover(); nil != err {
			log.Printf("Session|ReadPacket|%s|recover|FAIL|%s\n", self.remoteAddr, err)
		}
	}()

	//缓存本次包的数据
	packetBuff := make([]byte, 0, self.rc.ReadBufferSize)
	buff := bytes.NewBuffer(packetBuff)

	for !self.isClose {
		slice, err := self.br.ReadSlice(protocol.CMD_CRLF[0])
		//如果没有达到请求头的最小长度则继续读取
		if nil != err {
			buff.Reset()
			//链接是关闭的
			if err == io.EOF ||
				err == syscall.EPIPE ||
				err == syscall.ECONNRESET {
				self.Close()
				log.Printf("Session|ReadPacket|%s|\\r|FAIL|CLOSE SESSION|%s\n", self.remoteAddr, err)
			}
			continue
		}

		if buff.Len()+len(slice) >= protocol.MAX_PACKET_BYTES {
			log.Printf("Session|ReadPacket|%s|WRITE|TOO LARGE|CLOSE SESSION|%s\n", self.remoteAddr, err)
			self.Close()
			return
		}

		lflen, err := buff.Write(slice)
		//如果写满了则需要增长
		if lflen != len(slice) {
			buff.Write(slice[lflen:])
		}

		//读取下一个字节
		delim, err := self.br.ReadByte()
		if nil != err {
			buff.Reset()
			//链接是关闭的
			if err == io.EOF ||
				err == syscall.EPIPE ||
				err == syscall.ECONNRESET {
				self.Close()
				log.Printf("Session|ReadPacket|%s|\\r|FAIL|CLOSE SESSION|%s\n", self.remoteAddr, err)
			}
			continue
		}

		//写入，如果数据太大直接有ErrTooLarge则关闭session退出
		err = buff.WriteByte(delim)
		if nil != err {
			self.Close()
			log.Printf("Session|ReadPacket|%s|WRITE|TOO LARGE|CLOSE SESSION|%s\n", self.remoteAddr, err)
			return
		}

		//如果是\n那么就是一个完整的包
		if buff.Len() > protocol.PACKET_HEAD_LEN && delim == protocol.CMD_CRLF[1] {

			packet, err := protocol.UnmarshalTLV(buff.Bytes())

			if nil != err || nil == packet {
				log.Printf("Session|ReadPacket|UnmarshalTLV|FAIL|%s|%d|%s\n", err, buff.Len(), buff.Bytes())
				buff.Reset()
				continue
			}

			//写入缓冲
			self.ReadChannel <- *packet
			//重置buffer
			buff.Reset()
			self.idleTimer.Reset(self.rc.IdleTime)
			if nil != self.rc.FlowStat {
				self.rc.FlowStat.ReadFlow.Incr(1)
			}

		}
	}
}

//写出数据
func (self *Session) Write(packet protocol.Packet) error {
	defer func() {
		if err := recover(); nil != err {
			log.Printf("Session|Write|%s|recover|FAIL|%s\n", self.remoteAddr, err)
		}
	}()

	if !self.isClose {
		select {
		case self.WriteChannel <- packet:
			return nil
		default:
			return errors.New(fmt.Sprintf("WRITE CHANNLE [%s] FULL", self.remoteAddr))
		}
	}
	return errors.New(fmt.Sprintf("Session|[%s]|CLOSED", self.remoteAddr))
}

//真正写入网络的流
func (self *Session) write0(tlv protocol.Packet) {

	packet := protocol.MarshalPacket(&tlv)
	if nil == packet || len(packet) <= 0 {
		log.Printf("Session|write0|MarshalPacket|FAIL|EMPTY PACKET|%s\n", tlv)
		//如果是同步写出
		return
	}

	length, err := self.bw.Write(packet)
	if nil != err {
		log.Printf("Session|write0|conn|%s|FAIL|%s|%d/%d\n", self.remoteAddr, err, length, len(packet))
		//链接是关闭的
		if err == io.EOF ||
			err == syscall.EPIPE || err == syscall.ECONNRESET {
			self.Close()
			return
		}

		//如果没有写够则再写一次
		if err == io.ErrShortWrite {
			self.bw.Write(packet[length:])
		}
	}

	if nil != self.rc.FlowStat {
		self.rc.FlowStat.WriteFlow.Incr(1)
	}

}

//写入响应
func (self *Session) WritePacket() {

	var packet protocol.Packet
	ticker := time.NewTicker(5 * time.Millisecond)
	for !self.isClose {

		//写入网络
		select {
		//1.读取数据包
		case packet = <-self.WriteChannel:
			//写入网络
			self.write0(packet)
			self.idleTimer.Reset(self.rc.IdleTime)
		case <-ticker.C:
			self.flush()

		}
	}
	ticker.Stop()
}

func (self *Session) flush() {
	if self.bw.Buffered() > 0 {
		err := self.bw.Flush()
		if nil != err {
			log.Printf("Session|Write|FLUSH|FAIL|%t\n", err.Error())
			self.bw.Reset(self.conn)
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
		self.conn.Close()
		close(self.WriteChannel)
		close(self.ReadChannel)
		log.Printf("Session|Close|%s...\n", self.remoteAddr)
	}
	return nil
}
