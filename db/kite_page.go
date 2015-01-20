package db

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	// "log"
)

const PAGE_TYPE_PART = 0
const PAGE_TYPE_END = 1
const PAGE_HEADER_SIZE = 4 + 4 + 4 + 4

type KiteDBPage struct {
	pageId   int    // 每页一个id
	checksum uint32 // 这页数据的校验和，保证消息的完整性
	pageType int    // 这块数据后续还有页，还是已经包含全部数据
	next     int    // 这页的下一页的位置
	data     []byte // 这页的数据
}

func NewKiteDBPage() *KiteDBPage {
	return &KiteDBPage{}
}

func (self *KiteDBPage) getWriteFileNo() int {
	return self.pageId / PAGE_FILE_PAGE_COUNT
}

func (self *KiteDBPage) getOffset() int64 {
	pageN := self.pageId % PAGE_FILE_PAGE_COUNT
	return int64(PAGE_FILE_HEADER_SIZE + pageN*PAGE_FILE_PAGE_SIZE)
}

func (self *KiteDBPage) setChecksum() {
	h := crc32.NewIEEE()
	h.Write(self.data)
	self.checksum = h.Sum32()
}

func (self *KiteDBPage) getNext() int {
	pageN := (self.pageId + 1) % PAGE_FILE_PAGE_COUNT
	return PAGE_FILE_HEADER_SIZE + pageN*PAGE_FILE_PAGE_SIZE
}

func (self *KiteDBPage) ToBinary() []byte {
	length := PAGE_HEADER_SIZE + len(self.data)
	// log.Println("binary length", length)
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	binary.Write(buff, binary.BigEndian, uint32(self.pageId))
	binary.Write(buff, binary.BigEndian, uint32(self.pageType))
	binary.Write(buff, binary.BigEndian, uint32(self.checksum))
	binary.Write(buff, binary.BigEndian, uint32(len(self.data)))
	binary.Write(buff, binary.BigEndian, self.data)
	padding := PAGE_FILE_PAGE_SIZE - PAGE_HEADER_SIZE - len(self.data)
	if padding > 0 {
		bs := make([]byte, padding)
		binary.Write(buff, binary.BigEndian, bs)
	}
	return buff.Bytes()
}
