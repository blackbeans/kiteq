package mysql

import (
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	. "kiteq/store"
	"log"
	"strconv"
	"strings"
	"time"
)

var SQL_BATCH_UPDATE = "update kite_msg_{} set succ_groups=?,fail_groups=?,next_deliver_time=?,deliver_count=? where message_id=?"
var SQL_BATCH_DELETE = "delete from kite_msg_{} where message_id=?"

func (self *KiteMysqlStore) start() {

	for i := 0; i < self.hashshard.ShardCnt(); i++ {

		sqld := strings.Replace(SQL_BATCH_DELETE, "{}", strconv.Itoa(i), -1)
		sqlu := strings.Replace(SQL_BATCH_UPDATE, "{}", strconv.Itoa(i), -1)
		// log.Printf("KiteMysqlStore|start|SQL|%s\n|%s\n", sqlu, sqld)
		go self.startBatch(sqld, sqlu, self.batchUpChan[i], self.batchDelChan[i])
	}

}

//批量删除任务
func (self *KiteMysqlStore) startBatch(prepareDelSQL, prepareUpSQL string, chu chan *MessageEntity, chd chan string) {
	batchu := make([]*MessageEntity, 0, self.batchUpSize)
	batchd := make([]string, 0, self.batchDelSize)
	timer := time.NewTimer(self.flushPeriod)
	flush := false
	for {
		select {
		case mid := <-chd:
			batchd = append(batchd, mid)
		case entity := <-chu:
			batchu = append(batchu, entity)
		case <-timer.C:
			flush = true
		}

		if len(batchu) > self.batchUpSize || len(batchd) > self.batchDelSize || flush {
			//强制提交: 达到批量提交的阀值或者超时没有数据则提交
			self.batchUpdate(prepareUpSQL, batchu)
			self.batchDelete(prepareDelSQL, batchd)
			batchu = batchu[:0]
			batchd = batchd[:0]
			flush = false
			timer.Reset(self.flushPeriod)
		}
	}
	timer.Stop()
}

func (self *KiteMysqlStore) AsyncUpdate(entity *MessageEntity) {
	idx := self.hashshard.FindForKey(entity.MessageId)
	self.batchUpChan[idx] <- entity
}
func (self *KiteMysqlStore) AsyncDelete(messageid string) {
	idx := self.hashshard.FindForKey(messageid)
	self.batchDelChan[idx] <- messageid
}

func (self *KiteMysqlStore) batchDelete(prepareSQL string, messageId []string) bool {

	if len(messageId) <= 0 {
		return true
	}

	tx, err := self.dbmap.Begin()
	if nil != err {
		log.Printf("KiteMysqlStore|batchDelete|Tx|Begin|FAIL|%s\n", err)
		return false
	}

	stmt, err := tx.Prepare(prepareSQL)
	if nil != err {
		log.Printf("KiteMysqlStore|batchDelete|Prepare|FAIL|%s|%s\n", err, prepareSQL)
		return false
	}

	defer stmt.Close()

	for _, v := range messageId {
		_, err = stmt.Exec(v)
		if nil != err {
			log.Printf("KiteMysqlStore|batchDelete|FAIL|%s|%s\n", err, v)
		}
	}

	err = tx.Commit()
	if nil != err {
		tx.Rollback()
	}
	return nil == err
}

func (self *KiteMysqlStore) batchUpdate(prepareSQL string, entity []*MessageEntity) bool {

	if len(entity) <= 0 {
		return true
	}

	tx, err := self.dbmap.Begin()
	if nil != err {
		log.Printf("KiteMysqlStore|batchUpdate|Tx|Begin|FAIL|%s\n", err)
		return false
	}

	stmt, err := tx.Prepare(prepareSQL)
	if nil != err {
		log.Printf("KiteMysqlStore|batchUpdate|Prepare|FAIL|%s|%s\n", err, prepareSQL)
		return false
	}
	defer stmt.Close()

	args := make([]interface{}, 0, 5)
	var errs error
	for _, e := range entity {

		args = args[:0]

		sg, err := json.Marshal(e.SuccGroups)
		if nil != err {
			log.Printf("KiteMysqlStore|batchUpdate|SUCC GROUP|MARSHAL|FAIL|%s|%s|%s\n", err, e.MessageId, e.SuccGroups)
			errs = err
			continue
		}

		args = append(args, sg)

		fg, err := json.Marshal(e.FailGroups)
		if nil != err {
			log.Printf("KiteMysqlStore|batchUpdate|FAIL GROUP|MARSHAL|FAIL|%s|%s|%s\n", err, e.MessageId, e.FailGroups)
			errs = err
			continue
		}

		args = append(args, fg)

		//设置一下下一次投递时间
		args = append(args, e.NextDeliverTime)

		args = append(args, e.DeliverCount)

		args = append(args, e.MessageId)

		_, err = stmt.Exec(args...)
		if nil != err {
			log.Printf("KiteMysqlStore|batchUpdate|FAIL|%s|%s\n", err, e)
			errs = err
		}

	}
	errs = tx.Commit()
	if nil != errs {
		log.Printf("KiteMysqlStore|batchUpdate|COMMIT|FAIL|%s\n", errs)
		tx.Rollback()
	}
	return nil == errs
}
