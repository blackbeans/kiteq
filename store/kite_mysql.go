package store

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
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
  `expiredTime` BIGINT NULL,
  `groupId` VARCHAR(45) NULL,
  `commit` TINYINT NULL DEFAULT 0 COMMENT '提交状态',
  `body` BLOB NULL,
  `topo` TINYINT NULL DEFAULT 0 COMMENT '在哪个拓扑里',
  INDEX `idx_commit` (`commit` ASC),
  INDEX `idx_msgId` (`messageId` ASC),
  PRIMARY KEY (`id`))
ENGINE = InnoDB;
*/
type KiteMysqlStore struct {
	addr string
	db   *sql.DB
}

func NewKiteMysql(addr string) *KiteMysqlStore {
	db, err := sql.Open("mysql", addr)
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(1024)
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
	stmt, err := self.db.Prepare("SELECT messageId,topic,messageType,groupId,expiredTime,commit,body FROM `kite_msg` WHERE `messageId` = ?")
	if err != nil {
		log.Println(err)
		return nil
	}
	defer stmt.Close()
	pk := messageId

	entity := &MessageEntity{}
	var groupId string
	err = stmt.QueryRow(pk).Scan(
		&entity.messageId,
		&entity.topic,
		&entity.messageType,
		&groupId,
		&entity.expiredTime,
		&entity.commit,
		&entity.body,
	)
	if err != nil {
		log.Println(err)
		return nil
	}
	entity.Header = &protocol.Header{
		MessageId:   proto.String(entity.messageId),
		Topic:       proto.String(entity.topic),
		MessageType: proto.String(entity.messageType),
		GroupId:     proto.String(groupId),
		ExpiredTime: proto.Int64(entity.expiredTime),
		Commit:      proto.Bool(entity.commit),
	}
	return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	stmt, err := self.db.Prepare("INSERT INTO `kite_msg`(messageId,topic,messageType,groupId,expiredTime,commit,body) VALUES(?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Println(err)
		return false
	}
	defer stmt.Close()
	pk := entity.messageId

	if _, err := stmt.Exec(pk, entity.topic, entity.messageType, entity.Header.GroupId, entity.expiredTime, entity.commit, entity.body); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Commit(messageId string) bool {
	stmt, err := self.db.Prepare("UPDATE `kite_msg` SET commit=? where messageId=?")
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
	stmt, err := self.db.Prepare("UPDATE `kite_msg` SET commit=? where messageId=?")
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
	stmt, err := self.db.Prepare("UPDATE `kite_msg` set topic=?, messageType=?, expiredTime=?, commit=?, body=? where messageId=?")
	if err != nil {
		log.Println(err)
		return false
	}
	defer stmt.Close()
	pk := entity.messageId

	if _, err := stmt.Exec(entity.topic, entity.messageType, entity.expiredTime, entity.commit, entity.body, pk); err != nil {
		log.Println(err)
		return false
	}
	return true
}
