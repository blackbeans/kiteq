package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)

type ITLVPacket interface {
	Marshal() []byte
	Unmarshal(packet []byte) error
}

//请求的packet
type RequestPacket struct {
	CmdType    uint8  //类型
	dataLength uint32 //数据长度
	Data       []byte
	Opaque     int32
}

func (self *RequestPacket) Marshal() []byte {
	//总长度	  4 字节+ 1字节 + 4字节 + var + \r + \n
	length := REQ_PACKET_HEAD_LEN + len(self.Data) + 2
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	binary.Write(buff, binary.BigEndian, self.Opaque) // 请求id
	//彻底包装request为TLV
	binary.Write(buff, binary.BigEndian, self.CmdType)           //数据类型
	binary.Write(buff, binary.BigEndian, uint32(len(self.Data))) //总数据包长度
	binary.Write(buff, binary.BigEndian, self.Data)              // 数据包
	binary.Write(buff, binary.BigEndian, CMD_CRLF)
	return buff.Bytes()
}

func (self *RequestPacket) Unmarshal(upacket []byte) error {

	packet := bytes.TrimRight(upacket, CMD_STR_CRLF)
	reader := bytes.NewReader(packet)

	err := binary.Read(reader, binary.BigEndian, &self.Opaque)
	if nil != err {
		return err
	}

	err = binary.Read(reader, binary.BigEndian, &self.CmdType)
	if nil != err {
		return err
	}

	err = binary.Read(reader, binary.BigEndian, &self.dataLength)
	if nil != err {
		return err
	}

	if self.dataLength > 0 {
		//读取数据包
		self.Data = make([]byte, self.dataLength, self.dataLength)
		err = binary.Read(reader, binary.BigEndian, self.Data)
		rl := uint32(len(self.Data))
		if nil != err || rl != self.dataLength {
			// log.Printf("RequestPacket|Unmarshal|Corrupt Data|%s|%d/%d|%t\n", err, rl,
			// 	self.dataLength, packet)
			return errors.New("Corrupt PacketData")
		}

	} else {
		log.Printf("RequestPacket|Unmarshal|NO Data|%t\n", self)
	}

	return nil
}

//返回响应packet
type ResponsePacket struct {
	CmdType    uint8
	Opaque     int32
	Status     int32
	RemoteAddr string
}

func (self *ResponsePacket) Marshal() []byte {
	addr := []byte(self.RemoteAddr)
	//总长度 4 + 4 + data + \r + \n
	length := RESP_PACKET_HEAD_LEN + len(addr) + 2
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	binary.Write(buff, binary.BigEndian, self.Opaque) // 请求id
	//彻底包装response
	binary.Write(buff, binary.BigEndian, self.CmdType)      //回馈包的类型
	binary.Write(buff, binary.BigEndian, self.Status)       //数据类型
	binary.Write(buff, binary.BigEndian, uint32(len(addr))) //总数据包长度
	binary.Write(buff, binary.BigEndian, addr)              // 数据包
	binary.Write(buff, binary.BigEndian, CMD_CRLF)
	return buff.Bytes()
}

func (self *ResponsePacket) Unmarshal(upacket []byte) error {
	packet := bytes.TrimRight(upacket, CMD_STR_CRLF)
	reader := bytes.NewReader(packet)
	var dl uint32

	err := binary.Read(reader, binary.BigEndian, &self.Opaque)
	if nil != err {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &self.CmdType)
	if nil != err {
		return err
	}

	err = binary.Read(reader, binary.BigEndian, &self.Status)
	if nil != err {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &dl)
	if nil != err {
		return err
	}

	addr := make([]byte, dl, dl)
	err = binary.Read(reader, binary.BigEndian, addr)
	wl := uint32(len(addr))
	if nil != err || wl != dl {
		// log.Printf("ResponsePacket|Unmarshal|Corrupt Data|%s|%d/%d|%t\n", err, wl, dl, packet)
		return errors.New("Corrupt PacketData")
	}
	self.RemoteAddr = string(addr)
	return nil
}
