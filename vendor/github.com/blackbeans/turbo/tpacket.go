package turbo

import (
	"bytes"
	"encoding/binary"
)



type Compress int8

//packet的包头部分
type PacketHeader struct {
	Opaque    int32 //请求的seqId
	CmdType   uint8 //类型
	Version   int16 //协议的版本号
	Extension int64 //扩展预留字段
	BodyLen   int32 //body的长度
}

func MarshalHeader(header PacketHeader, bodyLen int32) *bytes.Buffer {
	b := make([]byte, 0, PACKET_HEAD_LEN+bodyLen)
	buff := bytes.NewBuffer(b)
	//写入包头长度
	Write(buff, binary.BigEndian, header.Opaque)
	Write(buff, binary.BigEndian, header.CmdType)
	Write(buff, binary.BigEndian, header.Version)
	Write(buff, binary.BigEndian, header.Extension)
	Write(buff, binary.BigEndian, bodyLen)
	return buff
}

func UnmarshalHeader(r *bytes.Reader) (PacketHeader, error) {
	header := PacketHeader{}
	err := Read(r, binary.BigEndian, &(header.Opaque))
	if nil != err {
		return header, err
	}

	err = Read(r, binary.BigEndian, &(header.CmdType))
	if nil != err {
		return header, err
	}

	err = Read(r, binary.BigEndian, &(header.Version))
	if nil != err {
		return header, err
	}

	err = Read(r, binary.BigEndian, &(header.Extension))
	if nil != err {
		return header, err
	}

	err = Read(r, binary.BigEndian, &(header.BodyLen))
	if nil != err {
		return header, err
	}

	return header, nil
}



//-----------请求的packet
type Packet struct {
	Header  PacketHeader
	Data    []byte
	PayLoad interface{}
	//packet的回调
	OnComplete func(err error)
}

func NewPacket(cmdtype uint8, data []byte) *Packet {
	h := PacketHeader{Opaque: -1, CmdType: cmdtype, BodyLen: int32(len(data))}
	return &Packet{Header: h,
		Data: data}
}

func (self *Packet) Reset() {
	self.Header.Opaque = -1
}

func NewRespPacket(opaque int32, cmdtype uint8, data []byte) *Packet {
	p := NewPacket(cmdtype, data)
	p.Header.Opaque = opaque
	return p
}

func (self *Packet) Marshal() []byte {
	dl := 0
	if nil != self.Data {
		dl = len(self.Data)
	}

	buff := MarshalHeader(self.Header, int32(dl))
	buff.Write(self.Data)
	return buff.Bytes()
}


// TContext 处理的上下文

//处理器
type THandler func(ctx *TContext) error

//接受消息
type IOHandler func(message Packet,err error)

type TContext struct{
	Client *TClient
	Message *Packet
	Err error //上下文的错误
}
