package session

import (
	"bufio"
	"bytes"
	// "encoding/binary"
	// "errors"
	"go-kite/protocol"
	"io"
	"log"
	"net"
	"time"
)

type Session struct {
	GroupId         string
	conn            *net.TCPConn //tcp的session
	remoteAddr      string
	heartbeat       int64       //心跳包的时间
	ReadChannel     chan []byte //request的channel
	WriteChannel    chan []byte //response的channel
	isClose         bool
	onPacketRecieve func(session *Session, packet []byte)
}

func NewSession(conn *net.TCPConn, remoteAddr string,
	onPacketRecieve func(session *Session, packet []byte)) *Session {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Second)
	conn.SetNoDelay(true)

	session := &Session{
		conn:            conn,
		heartbeat:       0,
		ReadChannel:     make(chan []byte, 100),
		WriteChannel:    make(chan []byte, 100),
		isClose:         false,
		remoteAddr:      remoteAddr,
		onPacketRecieve: onPacketRecieve}

	return session
}

func (self *Session) RemotingAddr() string {
	return self.remoteAddr
}

//设置本次心跳检测的时间
func (self *Session) SetHeartBeat(duration int64) {
	self.heartbeat = duration
}

func (self *Session) GetHeartBeat() int64 {
	return self.heartbeat
}

//读取
func (self *Session) ReadPacket() {

	br := bufio.NewReader(self.conn)
	//缓存本次包的数据
	packetBuff := make([]byte, 0, 1024)
	buff := bytes.NewBuffer(packetBuff)

	for !self.isClose {
		slice, err := br.ReadSlice(protocol.CMD_CRLF[0])
		//如果没有达到请求头的最小长度则继续读取
		if nil != err && err != io.EOF {
			log.Printf("Session|ReadPacket|\\r|FAIL|%s\n", err)
			buff.Reset()
			continue
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
			continue
		}
		buff.WriteByte(delim)
		if buff.Len() > protocol.REQ_PACKET_HEAD_LEN && delim == protocol.CMD_CRLF[1] {

			//如果是\n那么就是一个完整的包
			packet := make([]byte, 0, buff.Len())
			packet = append(packet, buff.Bytes()...)
			//写入缓冲
			self.ReadChannel <- packet
			//重置buffer
			buff.Reset()
		}
	}
}

func (self *Session) DispatcherPacket() {

	//500个读协程
	for i := 0; i < 100; i++ {
		go func() {
			//解析包
			for !self.isClose {

				//1.读取数据包
				packet := <-self.ReadChannel

				//2.处理一下包
				go self.onPacketRecieve(self, packet)
			}
		}()
	}
}

//写入响应
func (self *Session) WritePacket() {

	//分为100个协程处理写
	for i := 0; i < 100; i++ {
		go func() {
			for !self.isClose {
				packet := <-self.WriteChannel

				//并发去写
				go func() {
					length, err := self.conn.Write(packet)
					if nil != err || length != len(packet) {
						log.Printf("Session|WritePacket|FAIL|%s|%d/%d|%t\n", err, length, len(packet), packet)
					} else {
						// log.Printf("Session|WritePacket|SUCC|%t\n", packet)
					}
				}()
			}
		}()
	}
}

func (self *Session) Close() error {
	self.isClose = true
	self.conn.Close()
	return nil
}
