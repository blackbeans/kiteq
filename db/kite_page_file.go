package db

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

const PAGEFILE_SUFFIX = ".data"
const PAGE_FILE_HEADER_SIZE = 4 * 1024
const NEW_FREE_LIST_SIZE = 32
const PAGE_FILE_PAGE_COUNT = 32
const PAGE_FILE_PAGE_SIZE = 4 * 1024
const MAX_PAGE_FILES = 32

// 维护了一组数据文件
type KiteDBPageFile struct {
	path           string
	writeFile      map[int]*os.File
	pageSize       int                 //每页的大小 默认4K
	pageCount      int                 //每个PageFile文件包含page数量
	pageCache      map[int]*KiteDBPage //以页ID为索引的缓存
	pageCacheSize  int                 //页缓存大小
	writes         chan *KiteDBWrite   //刷盘队列
	writeStop      chan int
	allocLock      sync.Mutex     //主要是对分配Page的时候需要加锁，防止重复分配了一个Page
	freeList       *KiteRingQueue //空闲页 优先向这里写入
	nextFreePageId int            //空闲页的分配从这里开始
}

// 数据的写入做了个封装
type KiteDBWrite struct {
	page   *KiteDBPage //写到哪个页上
	data   []byte      //写的数据
	length int         //写入长度
}

func NewKiteDBPageFile(base string, dbName string) *KiteDBPageFile {
	dir := fmt.Sprintf("%s/%s", base, dbName)
	log.Println(dir)
	// 创建目录
	if _, err := os.Stat(dir); err != nil {
		if err := os.Mkdir(dir, 0777); err != nil {
			log.Fatal("create ", dir, " failed")
			return nil
		}
	}
	var mutex sync.Mutex
	ins := &KiteDBPageFile{
		path:          dir,
		pageSize:      PAGE_FILE_PAGE_SIZE,
		pageCount:     PAGE_FILE_PAGE_COUNT,
		pageCache:     make(map[int]*KiteDBPage),
		pageCacheSize: 1024, //4MB
		writes:        make(chan *KiteDBWrite),
		freeList:      NewKiteRingQueue(NEW_FREE_LIST_SIZE * 2),
		allocLock:     mutex,
	}
	last := 0
	// 判断是否是已经有数据
	if _, err := os.Stat(fmt.Sprintf("%s/%d%s", dir, last, PAGEFILE_SUFFIX)); err != nil {
		ins.writeFile = make(map[int]*os.File)
		f, err := os.OpenFile(fmt.Sprintf("%s/%d%s", dir, last, PAGEFILE_SUFFIX), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatal(err)
		}
		ins.writeFile[last] = f
	} else {
		log.Fatal("load data", err)
	}
	log.Println("write file create as ", ins.writeFile)
	fileInfo, err := ins.writeFile[last].Stat()
	if err != nil {
		log.Fatal(err)
	}
	ins.nextFreePageId = int(fileInfo.Size()-PAGE_FILE_HEADER_SIZE) / ins.pageSize
	go ins.pollWrite()
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
	log.Println("create ", count, "pages")
	pages := make([]*KiteDBPage, count)
	log.Println("create pages", pages)
	for i := 0; i < count; i = i + 1 {
		if self.freeList.Len() == 0 {
			// 重新分配新的freeList
			self.reAllocFreeList(NEW_FREE_LIST_SIZE)
		}

		pages[i], _ = self.freeList.Dequeue()
	}
	log.Println("create pages result", pages)
	return pages
}

func (self *KiteDBPageFile) Read(pageIds []int) (pages []*KiteDBPage) {
	result := []*KiteDBPage{}
	for _, pageId := range pageIds {
		page, contains := self.pageCache[pageId]
		if !contains {
			page = &KiteDBPage{
				pageId: pageId,
			}
			self.pageCache[pageId] = page
		}
		result = append(result, page)
	}
	return result
}

func (self *KiteDBPageFile) Write(pages []*KiteDBPage) {
	for _, page := range pages {
		// 写page缓存
		self.pageCache[page.pageId] = page
		log.Println("write async")
		self.writes <- &KiteDBWrite{
			page:   page,
			data:   page.data,
			length: len(page.data),
		}
	}
}

type KiteDBWriteBatch []*KiteDBWrite

func (self KiteDBWriteBatch) Len() int {
	return len(self)
}

func (self KiteDBWriteBatch) Less(i, j int) bool {
	return self[i].page.pageId < self[i].page.pageId
}

func (self KiteDBWriteBatch) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self *KiteDBPageFile) pollWrite() {
	writeStart := make(chan int, 1)
	writeQueue := make(chan KiteDBWriteBatch)
	list := make(KiteDBWriteBatch, 0, 32)
	go self.WriteBatch(writeQueue)

	go func() {
		// 批量写的策略
		for {
			time.Sleep(time.Second * 1)
			writeStart <- 1
		}
	}()
	for {
		select {
		case <-self.writeStop:
			return
		case <-writeStart:
			log.Println("write queue active", list)
			clone := make(KiteDBWriteBatch, len(list))
			copy(clone, list[:len(list)])
			writeQueue <- clone
			list = make(KiteDBWriteBatch, 0, 32)
		case pageWrite := <-self.writes:
			log.Println("append page write ", pageWrite.page.data)
			list = append(list, pageWrite)
		}
	}
}

func (self *KiteDBPageFile) WriteBatch(queue chan KiteDBWriteBatch) {
	for {
		select {
		case <-self.writeStop:
			return
		case l := <-queue:
			log.Println("write active", l)
			sort.Sort(l)
			log.Println("write sorted", l)
			for _, page := range l {
				no := page.page.getWriteFileNo()
				log.Println("write file no", no)
				var file *os.File
				if self.writeFile[no] == nil {
					file, _ = os.OpenFile(
						self.path+"/"+string(no)+PAGEFILE_SUFFIX,
						os.O_RDWR,
						0666)
					self.writeFile[no] = file
				} else {
					file = self.writeFile[no]
				}
				file.Seek(page.page.getOffset(), 0)
				bs := page.page.ToBinary()
				log.Println("write binary", bs)
				log.Println("write binary length", len(bs))

				n, err := file.Write(bs)
				if err != nil {
					log.Fatal(err)
				}
				log.Println("write ", n, " bytes")
			}
		}
	}
}
