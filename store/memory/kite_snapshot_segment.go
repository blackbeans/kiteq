package memory

import (
	"bufio"
	"encoding/binary"
	log "github.com/blackbeans/log4go"
	"os"
)

type ChunkFlag uint8

const (
	MAX_SEGEMENT_SIZE   = 64 * 1024 * 1024 //最大的分段大仙
	MAX_CHUNK_SIZE      = 128 * 1024       //最大的chunk
	SEGMENT_PREFIX      = "segement-"
	SEGMENT_IDX_SUFFIX  = ".idx"
	SEGMENT_DATA_SUFFIX = ".kiteq"
	CHUNK_HEADER        = 4 + 4 + 8 + 1 //|length 4byte|checksum 4byte|id 8byte|flag 1byte| data variant|

	NORMAL  ChunkFlag = 'n'
	DELETE  ChunkFlag = 'd'
	EXPIRED ChunkFlag = 'e'
)

//消息文件
type Segement struct {
	path     string
	name     string //basename_0000000000
	file     *os.File
	bw       *bufio.Writer
	br       *bufio.Reader
	sid      int64 //segment id
	offset   int64 //segment current offset
	byteSize int32 //segement size
	chunks   []*Chunk
}

func (self *Segement) Open() error {

	//move index to offset
	self.file.Seek(self.offset, 0)
	self.bw = bufio.NewWriter(self.file)
	self.br = bufio.NewReader(self.file)
	log.Info("Segement|Open|SUCC|%s\n", self.name)
	return nil
}

func (self *Segement) Close() error {

	err := self.bw.Flush()
	if nil != err {
		log.Error("Segement|Close|Writer|FLUSH|FAIL|%s|%s|%s\n", err, self.path, self.name)
	}

	err = self.file.Close()
	if nil != err {
		log.Error("Segement|Close|FAIL|%s|%s|%s\n", err, self.path, self.name)
	}
	return err
}

//apend data
func (self *Segement) Append(data []byte) error {
	l, err := self.bw.Write(data)
	if nil != err || l != len(data) {
		log.Error("Segement|Append|FAIL|%s|%d/%d\n", err, l, len(data))
		return err
	}
	self.bw.Flush()

	//move offset
	self.offset += int64(l)
	self.byteSize += int32(l)
	return nil
}

//----------------------------------------------------
//|length 4byte|checksum 4byte|id 8byte|flag 1byte| data variant|
//----------------------------------------------------
//存储块
type Chunk struct {
	length   int32
	checksum uint32
	flag     ChunkFlag //chunk状态
	id       int64
	data     []byte // data
}

func (self *Chunk) marshal() []byte {

	buff := make([]byte, self.length)

	//encode
	binary.BigEndian.PutUint32(buff[0:4], uint32(self.length))
	binary.BigEndian.PutUint32(buff[4:8], self.checksum)
	binary.BigEndian.PutUint64(buff[8:16], uint64(self.id))
	buff[16] = uint8(self.flag)
	copy(buff[17:], self.data)

	return buff
}

type Segments []*Segement

func (self Segments) Len() int { return len(self) }
func (self Segments) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self Segments) Less(i, j int) bool {
	return self[i].sid < self[j].sid
}
