package kite

import (
	"go-kite/store"
	"errors"
	"encoding/json"
	"math"
)

// 一个存储引擎
type KiteDB struct {
	dbs map[string]*KiteDBPageFile
	dir string
}

// 创建一个DB，指定一个存储目录
func NewKiteDB(string dir) *KiteDB {
	return &KiteDB{
		dir: dir,
	}
}

func (self *KiteDB) SelectDB(string dbName) (*KiteDBPageFile, error) {
	db, exists := self.dbs[dbName]
	if exists {
		return db, nil
	}
	self.dbs[dbName] := NewKiteDBPageFile(self.dir, dbName)
	if self.dbs[dbName] != nil {
		return self.dbs[dbName], nil
	}
	return nil, errors.New("select Db " + dbName + "failed")
}

func (self *KiteDB) GetSession() {
	return &KiteDBSession{
		db: self,
	}
}

// 一次数据库会话
type KiteDBSession struct {
	db *KiteDB
}

func (self *KiteDBSession) Query(messageId string) *MessageEntity {
}

func (self *KiteDBSession) Save(entity *MessageEntity) bool {
	db,err := self.db.SelectDB(entity.topic)
	if err != nil {
		return false
	}
	data,err := json.Marshal(entity)
	if err != nil {
		return false
	}
	length := len(data)
	pages := db.Allocate(math.Ceil(length/float(db.pageSize-PAGE_HEADER_SIZE)))
	for i:=0;i<len(pages);i++ {
		pages[i].data = data[i*(db.pageSize-PAGE_HEADER_SIZE):(i+1)*(db.pageSize-PAGE_HEADER_SIZE)]
		if i + 1 < len(pages) {
			pages[i].pageType = PAGE_TYPE_PART
		} else {
			pages[i].pageType = PAGE_TYPE_END
		}
	}
	// 没有写入磁盘，只是放入到了写入队列，同时放到PageCache里
	db.Write(pages)
}

func (self *KiteDBSession) Commite(messageId string) bool {
	msg := self.Query(messageId)
	msg.commited = true
	return self.UpdateEntity(msg)
}

func (self *KiteDBSession) Rollback(messageId string) bool {
	msg := self.Query(messageId)
	msg.commited = false
	return self.UpdateEntity(msg)
}

func (self *KiteDBSession) UpdateEntity(entity *MessageEntity) bool {
	return slef.Save(entity)
}
