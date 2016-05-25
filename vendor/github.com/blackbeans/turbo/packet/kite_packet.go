package packet

//请求的packet
type Packet struct {
	Header *PacketHeader
	Data   []byte
}

func NewPacket(cmdtype uint8, data []byte) *Packet {
	h := &PacketHeader{Opaque: -1, CmdType: cmdtype, BodyLen: int32(len(data))}
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
