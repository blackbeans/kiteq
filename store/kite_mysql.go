package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/golang/protobuf/proto"
	"github.com/sutoo/gorp"
	_ "kiteq/protocol"
	"log"
)

/**
CREATE DATABASE IF NOT EXISTS `kite` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE `kite` ;

CREATE TABLE IF NOT EXISTS `kite`.`kite_msg` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `messageId` CHAR(32) NOT NULL,
  `topic` VARCHAR(45) NULL DEFAULT 'default',
  `messageType` CHAR(4) NULL DEFAULT 0,
  `msgType` int(3) NOT NULL,
  `expiredTime` BIGINT NULL,
  `deliverCount` int(10) NOT NULL DEFAULT 0,
  `publishGroup` VARCHAR(45) NULL,
  `commit` TINYINT NULL DEFAULT 0 COMMENT '提交状态',
  `header` BLOB NOT NULL,
  `body` BLOB NOT NULL,
  `kiteq_server` char(32) NULL DEFAULT 0 COMMENT '在哪个拓扑里',
  INDEX `idx_commit` (`commit` ASC),
  INDEX `idx_msgId` (`messageId` ASC),
  PRIMARY KEY (`id`))
ENGINE = InnoDB;
*/
type KiteMysqlStore struct {
	addr  string
	dbmap *gorp.DbMap
}

func NewKiteMysql(addr string) *KiteMysqlStore {
	db, err := sql.Open("mysql", addr)
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(1024)
	if err != nil {
		log.Fatal("mysql can not connect")
	}

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}

	// add a table, setting the table name and
	// specifying that the Id property is an auto incrementing PK
	dbmap.AddTableWithName(MessageEntity{}, "kite_msg").SetKeys(false, "MessageId").SetHashKey("MessageId")
	dbmap.TypeConverter = CustomTypeConverter{}

	// create the table. in a production system you'd generally
	// use a migration tool, or create the tables via scripts
	err = dbmap.CreateTablesIfNotExists()
	if err != nil {
		log.Println("CreateTablesIfNotExists failed.")
	}
	ins := &KiteMysqlStore{
		addr:  addr,
		dbmap: dbmap,
	}
	return ins
}

//Converter for []string
type CustomTypeConverter struct {
}

func (me CustomTypeConverter) ToDb(val interface{}) (interface{}, error) {
	switch t := val.(type) {
	case []string:
		b, err := json.Marshal(t)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	return val, nil
}

func (me CustomTypeConverter) FromDb(target interface{}) (gorp.CustomScanner, bool) {
	switch target.(type) {
	case *[]string:
		binder := func(holder, target interface{}) error {
			s, ok := holder.(*string)
			if !ok {
				return errors.New("FromDb: Unable to convert to *string")
			}
			b := []byte(*s)
			return json.Unmarshal(b, target)
		}
		return gorp.CustomScanner{new(string), target, binder}, true
	}
	return gorp.CustomScanner{}, false
}

func (self *KiteMysqlStore) Query(messageId string) *MessageEntity {
	obj, err := self.dbmap.Get(MessageEntity{}, messageId, messageId)
	if err != nil {
		log.Println(err)
		return nil
	}
	return obj.(*MessageEntity)

	// 是否需要额外设置头部的状态i?????
	// //设置一下头部的状态
	// entity.Header.Commit = proto.Bool(entity.Commit)
	// return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	err := self.dbmap.Insert(entity)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Commit(messageId string) bool {
	entity := &MessageEntity{MessageId: messageId, Commit: true}
	_, err := self.dbmap.Update(entity)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Delete(messageId string) bool {
	entity := &MessageEntity{MessageId: messageId}
	_, err := self.dbmap.Delete(entity)
	if err != nil {
		log.Println(err)
		return false
	}

	return true
}

func (self *KiteMysqlStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}

func (self *KiteMysqlStore) UpdateEntity(entity *MessageEntity) bool {
	_, err := self.dbmap.Update(entity)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int32) (bool, []*MessageEntity) {
	return false, nil
}
