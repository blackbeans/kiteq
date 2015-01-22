package store

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

/**
CREATE DATABASE IF NOT EXISTS `kite` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE `kite` ;

CREATE TABLE IF NOT EXISTS `kite`.`kite_msg` (
  `messageId` INT NOT NULL,
  `topic` VARCHAR(45) NULL DEFAULT 'default',
  `messageType` TINYINT NULL DEFAULT 0,
  `expiredTime` BIGINT NULL,
  `groupId` VARCHAR(45) NULL,
  `commited` TINYINT NULL DEFAULT 0 COMMENT '提交状态',
  `body` BLOB NULL,
  `topo` TINYINT NULL DEFAULT 0 COMMENT '在哪个拓扑里',
  PRIMARY KEY (`messageId`),
  INDEX `idx_commited` (`commited` ASC))
ENGINE = InnoDB;
*/
type KiteMysqlStore struct {
	addr string
	db   *sql.DB
}

func NewKiteMysql(addr string) *KiteMysqlStore {
	db, err := sql.Open("mysql", addr)
	if err != nil {
		log.Fatal("mysql can not connect")
	}
	ins := &KiteMysqlStore{
		addr: addr,
		db:   db,
	}
	return ins
}

func (self *KiteMysqlStore) Query(messageId string) *MessageEntity {
	stmt, err := self.db.Prepare("SELECT messageId,topic,messageType,expiredTime,commited,body FROM `kite_msg` WHERE `messageId` = ?")
	if err != nil {
		log.Println(err)
		return nil
	}
	defer stmt.Close()
	pk, err := strconv.Atoi(messageId)
	if err != nil {
		log.Println(err)
		return nil
	}
	entity := &MessageEntity{}
	err = stmt.QueryRow(pk).Scan(
		&entity.messageId,
		&entity.topic,
		&entity.messageType,
		&entity.expiredTime,
		&entity.commited,
		&entity.body,
	)
	if err != nil {
		log.Println(err)
		return nil
	}
	return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	stmt, err := self.db.Prepare("INSERT INTO `kite_msg`(messageId,topic,messageType,expiredTime,commited,body) VALUES(?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Println(err)
		return false
	}
	defer stmt.Close()
	pk, err := strconv.Atoi(entity.messageId)
	if err != nil {
		log.Println(err)
		return false
	}
	if _, err := stmt.Exec(pk, entity.topic, entity.messageType, entity.expiredTime, entity.commited, entity.body); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Commit(messageId string) bool {
	stmt, err := self.db.Prepare("UPDATE `kite_msg` SET commited=? where messageId=?")
	if err != nil {
		log.Println(err)
		return false
	}
	defer stmt.Close()
	if _, err := stmt.Exec(true, messageId); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Rollback(messageId string) bool {
	stmt, err := self.db.Prepare("UPDATE `kite_msg` SET commited=? where messageId=?")
	if err != nil {
		log.Println(err)
		return false
	}
	defer stmt.Close()
	if _, err := stmt.Exec(false, messageId); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) UpdateEntity(entity *MessageEntity) bool {
	stmt, err := self.db.Prepare("UPDATE `kite_msg` set topic=?, messageType=?, expiredTime=?, commited=?, body=? where messageId=?")
	if err != nil {
		log.Println(err)
		return false
	}
	defer stmt.Close()
	pk, err := strconv.Atoi(entity.messageId)
	if err != nil {
		log.Println(err)
		return false
	}
	if _, err := stmt.Exec(entity.topic, entity.messageType, entity.expiredTime, entity.commited, entity.body, pk); err != nil {
		log.Println(err)
		return false
	}
	return true
}
