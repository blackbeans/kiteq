package kite

import (
	"go-kite/store"
	"math"
)

const PAGE_TYPE_PART = 0
const PAGE_TYPE_END = 1
const PAGE_HEADER_SIZE = 4 + 4 + 4 + 4

type KiteDBPage struct {
	pageId   int    // 每页一个id
	checksum int    // 这页数据的校验和，保证消息的完整性
	pageType int    // 这块数据后续还有页，还是已经包含全部数据
	next     int    // 这页的下一页的位置
	data     []byte // 这页的数据
}

func NewKiteDBPage() *KiteDBPage {
	return &KiteDBPage{}
}

func (self *KiteDBPage) getWriteFileNo() int {
	return math.Floor(pageId / float(PAGE_FILE_PAGE_COUNT))
}

func (self *KiteDBPage) getOffset() int {
	pageN := pageId % PAGE_FILE_PAGE_COUNT
	return PAGE_FILE_HEADER_SIZE + pageN*PAGE_FILE_PAGE_SIZE
}

func (self *KiteDBPage) ToBinary() []byte {

}
