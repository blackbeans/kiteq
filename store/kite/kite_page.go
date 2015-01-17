package kite

import (
	"go-kite/store"
)

const PAGE_TYPE_PART = 0
const PAGE_TYPE_END = 1

type KiteDBPage struct {
	pageId   int64  // 每页一个id
	checksum int    // 这页数据的校验和
	pageType int    // 这块数据后续还有页，还是已经包含全部数据
	next     int64  // 这页的下一页的位置
	data     []byte // 这页的数据
}

func NewKiteDBPage() *KiteDBPage {
	return &KiteDBPage{}
}
