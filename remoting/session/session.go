package session

import (
	"bufio"
	"bytes"
	// "encoding/binary"
	"errors"
	"go-kite/protocol"
	"io"
	"log"
	"net"
	// "time"
)

type Session struct {
	GroupId         string
	conn            *net.TCPConn //tcp的session
	remoteAddr      *net.TCPAddr
	heartbeat       int64       //心跳包的时间
	RequestChannel  chan []byte //request的channel
	ResponseChannel chan []byte //response的channel
	isClose         bool
}

func NewSession(conn *net.TCPConn, remoteAddr *net.TCPAddr) *Session {
	session := &Session{
		conn:            conn,
		heartbeat:       0,
		RequestChannel:  make(chan []byte, 100),
		ResponseChannel: make(chan []byte, 100),
		isClose:         false, remoteAddr: remoteAddr}

	return session
}

func (self *Session) RemotingAddr() *net.TCPAddr {
	return self.remoteAddr
}

var ERR_PACKET = errors.New("INALID PACKET!")

//读取
func (self *Session) ReadPacket() {

	br := bufio.NewReader(self.conn)
	//缓存本次包的数据
	packetBuff := make([]byte, 0, 1024)
	buff := bytes.NewBuffer(packetBuff)

	for !self.isClose {
		slice, err := br.ReadSlice(protocol.CMD_CRLF[0])
		//读取包
		if err == io.EOF {
			continue
		} else if nil != err {
			log.Printf("Session|ReadPacket|\\r|FAIL|%s\n", err)
			continue
		}

		_, err = buff.Write(slice)
		//数据量太庞大直接拒绝
		if buff.Len() >= protocol.MAX_PACKET_BYTES ||
			bytes.ErrTooLarge == err {
			log.Printf("Session|ReadPacket|ErrTooLarge|%d\n", buff.Len())
			buff.Reset()
			continue
		}

		//再读取一个字节判断是否为\n
		delim, err := br.ReadByte()
		if nil != err {
			log.Printf("Session|ReadPacket|\\n|FAIL|%s\n", err)
		} else {
			//继续读取
			buff.WriteByte(delim)
			if delim == protocol.CMD_CRLF[1] {
				//如果是\n那么就是一个完整的包
				packet := make([]byte, 0, buff.Len())
				packet = append(packet, buff.Bytes()...)
				self.onPacketRecieve(packet)
				//重置buffer
				buff.Reset()
			}
		}

	}
}

//请求包接收到
func (self *Session) onPacketRecieve(packet []byte) {
	// log.Printf("Session|onPacketRecieve|LOG|%t\n", packet)
	self.RequestChannel <- packet
}

//设置本次心跳检测的时间
func (self *Session) SetHeartBeat(duration int64) {
	self.heartbeat = duration
}

func (self *Session) GetHeartBeat() int64 {
	return self.heartbeat
}

var ERR_WRITE_PACKET = errors.New("WRITE PACKET FAIL ! (len != wlen)")

//写入响应
func (self *Session) WriteReponse(resp protocol.ITLVPacket) error {
	packet := resp.Marshal()
	// log.Printf("Session|WriteReponse|%t\n", packet)
	length, err := self.conn.Write(packet)
	if nil != err {
		return err
	} else if length != len(packet) {
		return ERR_WRITE_PACKET
	}
	return nil
}

//写出去ack
func (self *Session) WriteHeartBeatAck(ack *protocol.HeartBeatACKPacket) error {

	return nil
}

func (self *Session) Close() error {
	self.isClose = true
	self.conn.Close()
	return nil
}
