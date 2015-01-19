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
	dir string
}

// 创建一个DB，指定一个存储目录
func NewKiteDB(dir string) *KiteDB {
	return &KiteDB{
		dir: dir,
		dbs: make(map[string]*KiteDBPageFile),
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
	return nil
}

func (self *KiteDBSession) Save(entity *store.MessageEntity) bool {
	db, err := self.db.SelectDB(entity.Topic)
	if err != nil {
		return false
	}
	data, err := json.Marshal(entity)
	if err != nil {
		return false
	}
	length := len(data)
	pageN := math.Ceil(float64(length) / float64(db.pageSize-PAGE_HEADER_SIZE))
	log.Println("page alloc ", pageN)
	pages := db.Allocate(int(pageN))
	for i := 0; i < len(pages); i++ {
		pages[i].data = make([]byte, db.pageSize-PAGE_HEADER_SIZE)
		if length < (i+1)*(db.pageSize-PAGE_HEADER_SIZE) {
			copy(pages[i].data, data[i*(db.pageSize-PAGE_HEADER_SIZE):length])
		} else {
			copy(pages[i].data, data[i*(db.pageSize-PAGE_HEADER_SIZE):(i+1)*(db.pageSize-PAGE_HEADER_SIZE)])
		}

		if i+1 < len(pages) {
			pages[i].pageType = PAGE_TYPE_PART
		} else {
			pages[i].pageType = PAGE_TYPE_END
		}
		pages[i].setChecksum()
	}
	// 没有写入磁盘，只是放入到了写入队列，同时放到PageCache里
	log.Println("write ", pages)
	db.Write(pages)
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
	return self.Save(entity)
}
