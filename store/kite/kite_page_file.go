package kite

import (
	"container/list"
	"fmt"
	"go-kite/store"
	"os"
	"sync"
)

const PAGEFILE_SUFFIX = ".data"
const PAGE_FILE_HEADER_SIZE = 4 * 1024
const NEW_FREE_LIST_SIZE = 32

// 维护了一组数据文件
type KiteDBPageFile struct {
	path           string
	writeFile      *os.File
	pageSize       int                   //每页的大小 默认4K
	pageCount      int                   //每个PageFile文件包含page数量
	pageCache      map[int64]*KiteDBPage //以页ID为索引的缓存
	pageCacheSize  int                   //页缓存大小
	writes         chan *KiteDBWrite     //刷盘队列
	allocLock      sync.Mutex            //主要是对分配Page的时候需要加锁，防止重复分配了一个Page
	freeList       *KiteRingQueue        //空闲页 优先向这里写入
	nextFreePageId int64                 //空闲页的分配从这里开始
}

// 数据的写入做了个封装
type KiteDBWrite struct {
	page   *KiteDBPage //写到哪个页上
	data   []byte      //写的数据
	length int         //写入长度
}

func NewKiteDBPageFile(base string, topic string) *KiteDBPageFile {
	dir = fmt.Sprintf("%s/%s", base, topic)
	// 创建目录
	if _, err := os.Stat(dir); err != nil {
		if err := os.Mkdir(dir, 0777); err != nil {
			return nil, errors
		}
	}
	ins = &KiteDBPageFile{
		path:          dir,
		pageSize:      1024 * 4,
		pageCache:     make(map[int64]*KiteDBPage),
		pageCacheSize: 1024, //4MB
		writes:        make(chan *KiteDBWrite),
		freeList:      NewKiteRingQueue(NEW_FREE_LIST_SIZE * 2),
		allocLock:     sync.Mutex,
	}
	// 判断是否是已经有数据
	if _, err := os.Stat(dir + "/0" + PAGEFILE_SUFFIX); err != nil {

	} else {
		ins.writeFile, _ = os.Open(dir + "/0" + PAGEFILE_SUFFIX)
	}
	ins.nextFreePageId = (ins.writeFile.Stat().Size() - PAGE_FILE_HEADER_SIZE) / ins.pageSize
	return ins
}

func (self *KiteDBPageFile) reAllocFreeList(size int) {
	for i := 0; i < size; i++ {
		self.freeList.Enqueue(&KiteDBPage{
			pageId: self.nextFreePageId + i,
		})
	}
	self.nextFreePageId += size
}

func (self *KiteDBPageFile) Allocate(count int) []*KiteDBPage {
	self.allocLock.Lock()
	defer self.allocLock.Unlock()
	pages = new[count] * KiteDBPage
	for i := 0; i < count; i = i + 1 {
		if self.freeList.Len() == 0 {
			// 重新分配新的freeList
			self.reAllocFreeList(NEW_FREE_LIST_SIZE)
		}
		pages[i] = self.freeList.Dequeue()
	}
	return pages
}

func (self *KiteDBPageFile) Read(page *KiteDBPage) {

}

func (self *KiteDBPageFile) Write(page *KiteDBPage) {

}
