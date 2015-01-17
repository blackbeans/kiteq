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
	pages := db.Allocate(math.Ceil(length/db.pageSize))
}

func (self *KiteDBSession) Commite(messageId string) bool {

}

func (self *KiteDBSession) Rollback(messageId string) bool {

}

func (self *KiteDBSession) UpdateEntity(entity *MessageEntity) bool {

}
