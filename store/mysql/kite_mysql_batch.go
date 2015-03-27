package mysql

import (
	"encoding/json"
	. "kiteq/store"
	"log"
	"strconv"
	"strings"
	"time"
)

var SQL_BATCH_UPDATE = "update kite_msg_{} set succ_groups=?,fail_groups=?,next_deliver_time=?,deliver_count=? where message_id=?"
var SQL_BATCH_DELETE = "delete from kite_msg_{} where message_id=?"
var SQL_BATCH_COMMIT = "update kite_msg_{} set commit=1 where message_id=?"

func (self *KiteMysqlStore) start() {

	for i := 0; i < self.sqlwrapper.hashshard.ShardCnt(); i++ {

		sqld := strings.Replace(SQL_BATCH_DELETE, "{}", strconv.Itoa(i), -1)
		sqlu := strings.Replace(SQL_BATCH_UPDATE, "{}", strconv.Itoa(i), -1)
		sqlc := strings.Replace(SQL_BATCH_COMMIT, "{}", strconv.Itoa(i), -1)
		// log.Printf("KiteMysqlStore|start|SQL|%s\n|%s\n", sqlu, sqld)
		go self.startBatch(sqld, sqlu, sqlc, self.batchUpChan[i],
			self.batchDelChan[i], self.batchComChan[i])
	}

}

//批量删除任务
func (self *KiteMysqlStore) startBatch(prepareDelSQL, prepareUpSQL, prepareCommitSQL string,
	chu chan *MessageEntity, chd, chcommit chan string) {

	//启动的entity更新的携程
	go func(sql string, ch chan *MessageEntity, batchSize int,
		do func(sql string, d []*MessageEntity) bool) {
		timer := time.NewTimer(self.flushPeriod)
		data := make([]*MessageEntity, 0, batchSize)
		flush := false
		for {
			select {
			case mid := <-ch:
				data = append(data, mid)
			case <-timer.C:
				flush = true
			}
			//强制提交: 达到批量提交的阀值或者超时没有数据则提交
			if len(data) >= batchSize || flush {
				do(sql, data)
				data = data[:0]
				flush = false
				timer.Reset(self.flushPeriod)
			}
		}
		timer.Stop()
	}(prepareUpSQL, chu, self.batchUpSize, self.batchUpdate)

	batchFun := func(sql string, ch chan string, batchSize int,
		do func(sql string, d []string) bool) {
		timer := time.NewTimer(self.flushPeriod)
		data := make([]string, 0, batchSize)
		flush := false
		for {
			select {
			case mid := <-ch:
				data = append(data, mid)
			case <-timer.C:
				flush = true
			}
			//强制提交: 达到批量提交的阀值或者超时没有数据则提交
			if len(data) >= batchSize || flush {
				do(sql, data)
				data = data[:0]
				flush = false
				timer.Reset(self.flushPeriod)
			}
		}
		timer.Stop()
	}

	//启动批量删除
	go batchFun(prepareDelSQL, chd, self.batchDelSize, self.batchDelete)
	//启动批量提交
	go batchFun(prepareCommitSQL, chcommit, self.batchUpSize, self.batchCommit)

}

func (self *KiteMysqlStore) AsyncCommit(messageid string) bool {
	idx := self.sqlwrapper.hashshard.FindForKey(messageid)
	select {
	case self.batchComChan[idx] <- messageid:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (self *KiteMysqlStore) AsyncUpdate(entity *MessageEntity) bool {
	idx := self.sqlwrapper.hashshard.FindForKey(entity.MessageId)
	select {
	case self.batchUpChan[idx] <- entity:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}

}
func (self *KiteMysqlStore) AsyncDelete(messageid string) bool {
	idx := self.sqlwrapper.hashshard.FindForKey(messageid)
	select {
	case self.batchDelChan[idx] <- messageid:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (self *KiteMysqlStore) batchCommit(prepareSQL string, messageId []string) bool {

	if len(messageId) <= 0 {
		return true
	}

	log.Printf("KiteMysqlStore|batchCommit|%s|%s\n", prepareSQL, messageId)
	tx, err := self.db.Begin()
	if nil != err {
		log.Printf("KiteMysqlStore|batchCommit|Tx|Begin|FAIL|%s\n", err)
		return false
	}

	stmt, err := tx.Prepare(prepareSQL)
	if nil != err {
		log.Printf("KiteMysqlStore|batchCommit|Prepare|FAIL|%s|%s\n", err, prepareSQL)
		return false
	}

	defer stmt.Close()

	for _, v := range messageId {
		_, err = stmt.Exec(v)
		if nil != err {
			log.Printf("KiteMysqlStore|batchCommit|FAIL|%s|%s\n", err, v)
		}
	}

	err = tx.Commit()
	if nil != err {
		tx.Rollback()
	}
	return nil == err
}

func (self *KiteMysqlStore) batchDelete(prepareSQL string, messageId []string) bool {

	if len(messageId) <= 0 {
		return true
	}

	tx, err := self.db.Begin()
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

	tx, err := self.db.Begin()
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
