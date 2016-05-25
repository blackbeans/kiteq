package codec

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/blackbeans/turbo/packet"
	"math"
)

type LineBasedCodec struct {
	MaxFrameLength int //最大的包大小
}

//读取数据
func (self LineBasedCodec) Read(reader *bufio.Reader) (*bytes.Buffer, error) {
	buffer := make([]byte, 0, self.MaxFrameLength/2)
	buff := bytes.NewBuffer(buffer)
	for {
		line, isPrefix, err := reader.ReadLine()
		if nil != err {
			return nil, errors.New("Read Packet Err " + err.Error())
		}

		//overflow
		if (buff.Len() + len(line)) >= self.MaxFrameLength {
			return nil, errors.New(fmt.Sprintf("Too Large Packet %d/%d", buff.Len(), self.MaxFrameLength))
		}

		if (buff.Cap() - buff.Len()) < len(line) {
			//不够写入的在扩充
			buff.Grow(int(math.Abs(float64(buff.Cap()-buff.Len())) * 1.5))

		}
		_, er := buff.Write(line)
		if nil != err {
			return nil, errors.New("Write Packet Into Buff  Err " + er.Error())
		}

		//finish read line
		if !isPrefix {
			return buff, nil
		} else {
			//not finish continue read line
		}
		//overflow
		if buff.Len() >= self.MaxFrameLength {
			return nil, errors.New(fmt.Sprintf("Too Large Packet %d/%d", buff.Len(), self.MaxFrameLength))
		}
	}

}

//反序列化
//包装为packet，但是头部没有信息
func (self LineBasedCodec) UnmarshalPacket(buff *bytes.Buffer) (*packet.Packet, error) {
	return packet.NewPacket(0, buff.Bytes()), nil
}

//序列化
//直接获取data
func (self LineBasedCodec) MarshalPacket(packet *packet.Packet) []byte {
	return packet.Data
}
