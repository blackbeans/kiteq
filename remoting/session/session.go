package session

import (
	"bufio"
	"bytes"
	"kiteq/protocol"
	"log"
	"net"
	"time"
)

type Session struct {
	conn         *net.TCPConn //tcp的session
	remoteAddr   string
	ReadChannel  chan []byte //request的channel
	WriteChannel chan []byte //response的channel
	isClose      bool
}

func NewSession(conn *net.TCPConn) *Session {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Second)
	conn.SetNoDelay(true)

	session := &Session{
		conn:         conn,
		ReadChannel:  make(chan []byte, 1000),
		WriteChannel: make(chan []byte, 1000),
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
	br := bufio.NewReader(self.conn)
	//缓存本次包的数据
	packetBuff := make([]byte, 0, 1024)
	buff := bytes.NewBuffer(packetBuff)

	for !self.isClose {
		slice, err := br.ReadSlice(protocol.CMD_CRLF[0])
		//如果没有达到请求头的最小长度则继续读取
		if nil != err {
			buff.Reset()
			self.Close()
			log.Printf("Session|ReadPacket|%s|\\r|FAIL|CLOSE SESSION|%s\n", self.remoteAddr, err)
			return
		}

		lflen, err := buff.Write(slice)
		//如果写满了则需要增长
		if lflen != len(slice) {
			//增长+1
			buff.Grow(len(slice) - lflen + 1)
			buff.Write(slice[lflen:])
		}

		//读取下一个字节
		delim, err := br.ReadByte()
		if nil != err {
			log.Printf("Session|ReadPacket|%s|\\n|FAIL|CLOSE SESSION|%s\n", self.remoteAddr, err)
			self.Close()
			return
		}

		//写入，如果数据太大直接有ErrTooLarge则关闭session退出
		err = buff.WriteByte(delim)
		if nil != err {
			log.Printf("Session|ReadPacket|%s|WRITE|TOO LARGE|CLOSE SESSION|%s\n", self.remoteAddr, err)
			self.Close()
			return
		}

		if buff.Len() > protocol.PACKET_HEAD_LEN && delim == protocol.CMD_CRLF[1] {

			//如果是\n那么就是一个完整的包
			packet := make([]byte, buff.Len())
			//拷贝数据
			copy(packet, buff.Bytes())
			//写入缓冲
			self.ReadChannel <- packet
			//重置buffer
			buff.Reset()

		}
	}
}

//写出数据
func (self *Session) Write(packet []byte) {
	defer func() {
		if err := recover(); nil != err {
			log.Printf("Session|WritePacket|%s|recover|FAIL|%s\n", self.remoteAddr, err)
		}
	}()
	self.WriteChannel <- packet
}

//写入响应
func (self *Session) WritePacket() {
	ch := self.WriteChannel
	for !self.isClose {

		//1.读取数据包
		packet := <-ch

		//2.处理一下包
		//并发去写
		length, err := self.conn.Write(packet)
		if nil != err {
			log.Printf("Session|WritePacket|%s|FAIL|%s|%d/%d|%t\n", self.remoteAddr, err, length, len(packet), packet)
			self.Closed()
		} else {
			// log.Printf("Session|WritePacket|SUCC|%t\n", packet)
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
