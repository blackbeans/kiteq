package session

import (
	"bufio"
	"bytes"
	"io"
	"kiteq/protocol"
	"log"
	"net"
	"time"
)

type Session struct {
	conn         *net.TCPConn //tcp的session
	remoteAddr   string
	br           *bufio.Reader
	bw           *bufio.Writer
	ReadChannel  chan *protocol.Packet //request的channel
	WriteChannel chan *protocol.Packet //response的channel
	isClose      bool
}

func NewSession(conn *net.TCPConn) *Session {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Second)
	conn.SetNoDelay(true)

	session := &Session{
		conn:         conn,
		br:           bufio.NewReaderSize(conn, 1024),
		bw:           bufio.NewWriterSize(conn, 1024),
		ReadChannel:  make(chan *protocol.Packet, 1000),
		WriteChannel: make(chan *protocol.Packet, 1000),
		isClose:      false,
		remoteAddr:   conn.RemoteAddr().String()}

	return session
}

func (self *Session) RemotingAddr() string {
	return self.remoteAddr
}

//读取
func (self *Session) ReadPacket() {

	defer func() {
		if err := recover(); nil != err {
			log.Printf("Session|ReadPacket|%s|recover|FAIL|%s\n", self.remoteAddr, err)
		}
	}()

	//缓存本次包的数据
	packetBuff := make([]byte, 0, 1024)
	buff := bytes.NewBuffer(packetBuff)

	for !self.isClose {
		slice, err := self.br.ReadSlice(protocol.CMD_CRLF[0])
		//如果没有达到请求头的最小长度则继续读取
		if nil != err {
			buff.Reset()
			log.Printf("Session|ReadPacket|%s|\\r|FAIL|CLOSE SESSION|%s\n", self.remoteAddr, err)
			if err == io.EOF {
				self.Close()
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
			if err == io.EOF {
				self.Close()
			}
			log.Printf("Session|ReadPacket|%s|\\r|FAIL|CLOSE SESSION|%s\n", self.remoteAddr, err)
			continue
		}

		//写入，如果数据太大直接有ErrTooLarge则关闭session退出
		err = buff.WriteByte(delim)
		if nil != err {
			log.Printf("Session|ReadPacket|%s|WRITE|TOO LARGE|CLOSE SESSION|%s\n", self.remoteAddr, err)
			self.Close()
			return
		}

		//如果是\n那么就是一个完整的包
		if buff.Len() > protocol.PACKET_HEAD_LEN && delim == protocol.CMD_CRLF[1] {

			packet, err := protocol.UnmarshalTLV(buff.Bytes())

			if nil != err || nil == packet {
				log.Printf("Session|ReadPacket|UnmarshalTLV|FAIL|%s|%d\n", err, buff.Len())
				buff.Reset()
				continue
			}

			//写入缓冲
			self.ReadChannel <- packet
			//重置buffer
			buff.Reset()

		}
	}
}

//写出数据
func (self *Session) Write(packet *protocol.Packet) {
	defer func() {
		if err := recover(); nil != err {
			log.Printf("Session|Write|%s|recover|FAIL|%s\n", self.remoteAddr, err)
		}
	}()

	if !self.isClose {
		//如果是异步写出的
		if !packet.IsBlockingWrite() {
			if !self.isClose {
				self.WriteChannel <- packet
			}
		} else {
			//如果是同步写出
			self.write0(packet)
			self.flush()
		}
	}
}

//真正写入网络的流
func (self *Session) write0(tlv *protocol.Packet) {

	packet := protocol.MarshalPacket(tlv)
	if nil == packet || len(packet) <= 0 {
		log.Printf("Session|write0|MarshalPacket|FAIL|EMPTY PACKET|%s\n", tlv)
		//如果是同步写出
		return
	}

	//2.处理一下包
	length, err := self.bw.Write(packet)
	// length, err := self.conn.Write(packet)
	if nil != err {
		log.Printf("Session|write0|%s|FAIL|%s|%d/%d\n", self.remoteAddr, err, length, len(packet))
		if err == io.EOF {
			self.Closed()
			return
		}

		self.bw.Reset(self.conn)
		if err == io.ErrShortWrite {
			self.bw.Write(packet[length:])
		}

	} else {
		// log.Printf("Session|write0|SUCC|%t\n", packet)
	}
}

//写入响应
func (self *Session) WritePacket() {
	ch := self.WriteChannel
	bcount := 0
	var packet *protocol.Packet
	timeout := 100 * time.Millisecond
	maxIdleCount := 10
	idleCount := 1
	timer := time.NewTimer(1 * time.Second)
	for !self.isClose {

		if idleCount >= maxIdleCount {
			idleCount = maxIdleCount
		}
		//最大空闲等待时间
		waitTime := time.Duration(int64(idleCount) * int64(timeout))
		timer.Reset(waitTime)
		select {
		//1.读取数据包
		case packet = <-ch:
			//写入网络
			if nil != packet {
				self.write0(packet)
				bcount++
				//100个包统一flush一下
				bcount = bcount % 100
				if bcount == 0 {
					self.flush()
				}
			}
			idleCount = 1
			//如果超过1ms没有要写的数据强制flush一下
		case <-timer.C:
			self.flush()
			idleCount++
		}
	}
	timer.Stop()
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
	}
	return nil
}
