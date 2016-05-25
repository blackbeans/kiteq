package packet

import (
	"bytes"
	"encoding/binary"
)

//packet的包头部分
type PacketHeader struct {
	Opaque    int32 //请求的seqId
	CmdType   uint8 //类型
	Version   int16 //协议的版本号
	Extension int64 //扩展预留字段
	BodyLen   int32 //body的长度
}

func MarshalHeader(header *PacketHeader, bodyLen int32) *bytes.Buffer {
	b := make([]byte, 0, 4+PACKET_HEAD_LEN+bodyLen)
	buff := bytes.NewBuffer(b)
	//写入包头长度

	Write(buff, binary.BigEndian, int32(PACKET_HEAD_LEN+bodyLen))
	Write(buff, binary.BigEndian, header.Opaque)
	Write(buff, binary.BigEndian, header.CmdType)
	Write(buff, binary.BigEndian, header.Version)
	Write(buff, binary.BigEndian, header.Extension)
	Write(buff, binary.BigEndian, bodyLen)
	return buff
}

func UnmarshalHeader(r *bytes.Reader) (*PacketHeader, error) {
	header := &PacketHeader{}
	err := Read(r, binary.BigEndian, &(header.Opaque))
	if nil != err {
		return nil, err
	}

	err = Read(r, binary.BigEndian, &(header.CmdType))
	if nil != err {
		return nil, err
	}

	err = Read(r, binary.BigEndian, &(header.Version))
	if nil != err {
		return nil, err
	}

	err = Read(r, binary.BigEndian, &(header.Extension))
	if nil != err {
		return nil, err
	}

	err = Read(r, binary.BigEndian, &(header.BodyLen))
	if nil != err {
		return nil, err
	}

	return header, nil
}
