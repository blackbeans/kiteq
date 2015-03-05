package session

import (
	"bufio"
	"bytes"
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
	ReadChannel  chan *protocol.Packet //request的channel
	WriteChannel chan *protocol.Packet //response的channel
	isClose      bool
}

func NewSession(conn *net.TCPConn) *Session {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Second)
	//禁用nagle
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
			self.write0(true, packet)
		}
	}
}

//真正写入网络的流
func (self *Session) write0(syncFlush bool, tlv *protocol.Packet) {

	packet := protocol.MarshalPacket(tlv)
	if nil == packet || len(packet) <= 0 {
		log.Printf("Session|write0|MarshalPacket|FAIL|EMPTY PACKET|%s\n", tlv)
		//如果是同步写出
		return
	}

	var writer io.Writer
	if syncFlush {
		writer = self.conn
	} else {
		writer = self.bw
	}

	length, err := writer.Write(packet)
	if nil != err {
		log.Printf("Session|write0|conn|%s|FAIL|%s|%d/%d\n", self.remoteAddr, err, length, len(packet))
		//链接是关闭的
		if err == io.EOF ||
			err == syscall.EPIPE || err == syscall.ECONNRESET {
			self.Close()
			return
		}

		//只有syncflush才reset
		if !syncFlush && nil != err {
			self.bw.Reset(self.conn)
		}
		//如果没有写够则再写一次
		if err == io.ErrShortWrite {
			writer.Write(packet[length:])
		}
	}

}

//写入响应
func (self *Session) WritePacket() {
	ch := self.WriteChannel
	bcount := 0
	var packet *protocol.Packet
	timeout := 1 * time.Second
	packetSize := 0
	tick := time.NewTicker(timeout)
	syncFlush := true //是否强制flush 包数比较小的时候直接开启强制flush
	//如果1s中的包数小于1000个则直接syncFlush
	for !self.isClose {
		//写入网络
		select {
		//1.读取数据包
		case packet = <-ch:
			//写入网络
			if nil != packet {
				bcount++
				packetSize += len(packet.Data)

				//每秒包大于512个字节
				if packetSize >= 128 {
					syncFlush = false
				} else {
					syncFlush = true
				}

				self.write0(syncFlush, packet)
				//1000个包统一flush一下
				bcount = bcount % 1000
				if bcount == 0 && !syncFlush {
					self.flush()
				}

			}
			//如果超过1s没有要写的数据强制flush一下
		case <-tick.C:
			self.flush()
			packetSize = 0
			bcount = 0
		}
	}
	tick.Stop()
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
