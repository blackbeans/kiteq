package db

import (
	"encoding/json"
	"errors"
	"go-kite/store"
	"log"
	"math"
)

// 一个存储引擎
type KiteDB struct {
	dbs map[string]*KiteDBPageFile
	idx *KiteBtreeIndex
	dir string
}

// 创建一个DB，指定一个存储目录
func NewKiteDB(dir string) *KiteDB {
	return &KiteDB{
		dir: dir,
		dbs: make(map[string]*KiteDBPageFile),
		idx: NewIndex(),
	}
}

func (self *KiteDB) SelectDB(dbName string) (*KiteDBPageFile, error) {
	db, exists := self.dbs[dbName]
	if exists {
		return db, nil
	}
	self.dbs[dbName] = NewKiteDBPageFile(self.dir, dbName)
	log.Println("new db", dbName)
	if self.dbs[dbName] != nil {
		return self.dbs[dbName], nil
	}
	return nil, errors.New("select Db " + dbName + "failed")
}

func (self *KiteDB) GetSession() *KiteDBSession {
	return &KiteDBSession{
		db: self,
	}
}

// 一次数据库会话
type KiteDBSession struct {
	db *KiteDB
}

func (self *KiteDBSession) Query(messageId string) *store.MessageEntity {
	// @todo 查询messageId的索引，得到pageId和topic
	item, _ := self.db.idx.Search(messageId)
	entity := &store.MessageEntity{
		MessageId: messageId,
		Topic:     item.topic,
	}
	db, err := self.db.SelectDB(entity.Topic)
	if err != nil {
		return nil
	}
	pageIds := []int{item.pageId}
	pages := db.Read(pageIds)
	bs := make([]byte, db.pageSize*len(pages))
	copyN := 0
	for _, page := range pages {
		copy(bs, page.data)
		copyN += len(page.data)
	}
	log.Println("query result data", bs[:copyN])
	json.Unmarshal(bs[:copyN], &entity)
	return entity
}

func (self *KiteDBSession) Save(entity *store.MessageEntity) bool {
	db, err := self.db.SelectDB(entity.Topic)
	if err != nil {
		return false
	}
	data, err := json.Marshal(entity)
	log.Println("marshal result", data)
	if err != nil {
		return false
	}
	length := len(data)
	pageN := math.Ceil(float64(length) / float64(db.pageSize-PAGE_HEADER_SIZE))
	log.Println("page alloc ", pageN)
	pages := db.Allocate(int(pageN))
	for i := 0; i < len(pages); i++ {
		if length < (i+1)*(db.pageSize-PAGE_HEADER_SIZE) {
			pages[i].data = make([]byte, length)
			copy(pages[i].data, data[i*(db.pageSize-PAGE_HEADER_SIZE):length])
		} else {
			pages[i].data = make([]byte, db.pageSize-PAGE_HEADER_SIZE)
			copy(pages[i].data, data[i*(db.pageSize-PAGE_HEADER_SIZE):(i+1)*(db.pageSize-PAGE_HEADER_SIZE)])
		}

		if i+1 < len(pages) {
			pages[i].pageType = PAGE_TYPE_PART
		} else {
			pages[i].pageType = PAGE_TYPE_END
		}
		pages[i].setChecksum()
		log.Println("page alloc end ", pages[i])
	}
	// 没有写入磁盘，只是放入到了写入队列，同时放到PageCache里
	log.Println("write ", pages)
	db.Write(pages)
	// @todo 建立messageId到pageId,topic的索引
	self.db.idx.Insert(entity.MessageId, &KiteIndexItem{
		pageId: pages[0].pageId,
		topic:  entity.Topic,
	})
	return true
}

func (self *KiteDBSession) Commite(messageId string) bool {
	msg := self.Query(messageId)
	msg.Commited = true
	return self.UpdateEntity(msg)
}

func (self *KiteDBSession) Rollback(messageId string) bool {
	msg := self.Query(messageId)
	msg.Commited = false
	return self.UpdateEntity(msg)
}

func (self *KiteDBSession) UpdateEntity(entity *store.MessageEntity) bool {
	// oldEntity := self.Query(entity.MessageId)
	return self.Save(entity)
}
