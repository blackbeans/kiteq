package remoting

import (
	// "bytes"
	"encoding/binary"
	"errors"
	"go-kite/remoting/protocol"
	"log"
	"net"
)

type Session struct {
	conn      *net.TCPConn //tcp的session
	heartbeat int64
}

func NewSession(conn *net.TCPConn) *Session {
	session := &Session{conn: conn}
	return session
}

var ERR_PACKET = errors.New("INALID PACKET!")

//读取
func (self *Session) ReadTLV() (uint8, []byte, error) {
	// 1字节 + 4字节 + var + 1 + 1
	var ptype uint8
	var length uint32
	var data []byte
	var err error
	var crlf [2]byte

	for i := 0; i < 3; i++ {
		//类型数据有问题
		err = binary.Read(self.conn, binary.BigEndian, &ptype)
		if nil != err {
			return ptype, data, err
		}

		err = binary.Read(self.conn, binary.BigEndian, &length)
		if nil != err {
			return ptype, data, err
		}

		data = make([]byte, length, length)
		err = binary.Read(self.conn, binary.BigEndian, data)
		//如果定义的length和真正读取的数据包的长度不一致则有问题
		if nil != err {
			return ptype, data, err
		}

		// log.Printf("Session|ReadTLV|%d|%d|%t\n", ptype, length, data)

		//判断一下当前读取的末尾是否为CRLF
		//说明数据包有错误
		if nil != binary.Read(self.conn, binary.BigEndian, &crlf) ||
			crlf != protocol.CMD_CRLF {
			err = ERR_PACKET
			log.Printf("Session|ReadTLV|INALID PACKET|%t\n", data)
		} else {
			err = nil
			break
		}
	}

	return ptype, data, err
}

//设置本次心跳检测的时间
func (self *Session) SetHeartBeat(duration int64) {
	self.heartbeat = duration
}

func (self *Session) GetHeartBeat() int64 {
	return self.heartbeat
}

//写入响应
func (self *Session) WriteReponse(resp *protocol.ResponsePacket) error {
	return nil
}

//写出去ack
func (self *Session) WriteHeartBeatAck(ack *protocol.HeartBeatACKPacket) error {

	return nil
}

func (self *Session) Close() error {
	self.conn.Close()
	return nil
}
