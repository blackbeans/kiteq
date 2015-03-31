package mysql

import (
	"database/sql"
	"encoding/json"
	log "github.com/blackbeans/log4go"
	. "kiteq/store"
	"time"
)

func (self *KiteMysqlStore) Start() {

	//创建每种批量的preparestmt
	stmts := make(map[batchType][]*StmtPool, 4)
	for k, v := range self.sqlwrapper.batchSQL {
		btype := k
		pool := make([]*StmtPool, 0, self.sqlwrapper.hashshard.ShardCnt())
		for _, s := range v {
			psql := s

			err, p := NewStmtPool(10, 20, 50, 1*time.Minute, func() (error, *sql.Stmt) {
				stmt, err := self.dbslave.Prepare(psql)
				if nil != err {
					log.Error("StmtPool|Create Stmt|FAIL|%s|%s\n", err, psql)
					return err, nil
				}
				return nil, stmt
			})
			if nil != err {
				log.Error("NewKiteMysql|NewStmtPool|FAIL|%s\n", err)
				panic(err)
			}
			pool = append(pool, p)
		}
		stmts[btype] = pool
	}

	self.stmtPools = stmts

	for i := 0; i < self.sqlwrapper.hashshard.ShardCnt(); i++ {
		// log.Printf("KiteMysqlStore|start|SQL|%s\n|%s\n", sqlu, sqld)
		go self.startBatch(i, self.batchUpChan[i],
			self.batchDelChan[i], self.batchComChan[i])
	}
	log.Info("KiteMysqlStore|Start...")
}

//批量删除任务
func (self *KiteMysqlStore) startBatch(hash int,
	chu chan *MessageEntity, chd, chcommit chan string) {

	//启动的entity更新的携程
	go func(hashId int, ch chan *MessageEntity, batchSize int,
		do func(sql int, d []*MessageEntity) bool) {
		timer := time.NewTimer(self.flushPeriod)
		data := make([]*MessageEntity, 0, batchSize)
		flush := false
		for !self.stop {
			select {
			case mid := <-ch:
				data = append(data, mid)
			case <-timer.C:
				flush = true
			}
			//强制提交: 达到批量提交的阀值或者超时没有数据则提交
			if len(data) >= batchSize || flush {
				do(hashId, data)
				data = data[:0]
				flush = false
				timer.Reset(self.flushPeriod)
			}
		}
		timer.Stop()
	}(hash, chu, self.batchUpSize, self.batchUpdate)

	batchFun := func(hashid int, ch chan string, batchSize int,
		do func(hashid int, d []string) bool) {
		timer := time.NewTimer(self.flushPeriod)
		data := make([]string, 0, batchSize)
		flush := false
		for !self.stop {
			select {
			case mid := <-ch:
				data = append(data, mid)
			case <-timer.C:
				flush = true
			}
			//强制提交: 达到批量提交的阀值或者超时没有数据则提交
			if len(data) >= batchSize || flush {
				do(hashid, data)
				data = data[:0]
				flush = false
				timer.Reset(self.flushPeriod)
			}
		}
		timer.Stop()
	}

	//启动批量删除
	go batchFun(hash, chd, self.batchDelSize, self.batchDelete)
	//启动批量提交
	go batchFun(hash, chcommit, self.batchUpSize, self.batchCommit)

}

func (self *KiteMysqlStore) AsyncCommit(messageid string) bool {
	idx := self.sqlwrapper.hashshard.FindForKey(messageid)
	self.batchComChan[idx] <- messageid
	return true

}

func (self *KiteMysqlStore) AsyncUpdate(entity *MessageEntity) bool {
	idx := self.sqlwrapper.hashshard.FindForKey(entity.MessageId)
	self.batchUpChan[idx] <- entity
	return true

}
func (self *KiteMysqlStore) AsyncDelete(messageid string) bool {
	idx := self.sqlwrapper.hashshard.FindForKey(messageid)
	self.batchDelChan[idx] <- messageid
	return true
}

func (self *KiteMysqlStore) batchCommit(hashId int, messageId []string) bool {

	if len(messageId) <= 0 {
		return true
	}

	// log.Printf("KiteMysqlStore|batchCommit|%s|%s\n", prepareSQL, messageId)
	p := self.stmtPools[COMMIT][hashId]
	err, stmt := p.Get()
	if nil != err {
		log.Error("KiteMysqlStore|batchCommit|GET STMT|FAIL|%s|%d\n", err, hashId)
		return false
	}
	defer p.Release(stmt)

	for _, v := range messageId {
		_, err = stmt.Exec(true, v)
		if nil != err {
			log.Error("KiteMysqlStore|batchCommit|FAIL|%s|%s\n", err, v)
		}
	}
	return nil == err
}

func (self *KiteMysqlStore) batchDelete(hashId int, messageId []string) bool {

	if len(messageId) <= 0 {
		return true
	}

	p := self.stmtPools[DELETE][hashId]
	err, stmt := p.Get()
	if nil != err {
		log.Error("KiteMysqlStore|batchDelete|GET STMT|FAIL|%s|%d\n", err, hashId)
		return false
	}
	defer p.Release(stmt)

	for _, v := range messageId {
		_, err = stmt.Exec(v)
		if nil != err {
			log.Error("KiteMysqlStore|batchDelete|FAIL|%s|%s\n", err, v)
		}
	}
	return nil == err
}

func (self *KiteMysqlStore) batchUpdate(hashId int, entity []*MessageEntity) bool {

	if len(entity) <= 0 {
		return true
	}

	p := self.stmtPools[UPDATE][hashId]
	err, stmt := p.Get()
	if nil != err {
		log.Error("KiteMysqlStore|batchUpdate|GET STMT|FAIL|%s|%d\n", err, hashId)
		return false
	}
	defer p.Release(stmt)

	args := make([]interface{}, 0, 5)
	var errs error
	for _, e := range entity {

		args = args[:0]

		sg, err := json.Marshal(e.SuccGroups)
		if nil != err {
			log.Error("KiteMysqlStore|batchUpdate|SUCC GROUP|MARSHAL|FAIL|%s|%s|%s\n", err, e.MessageId, e.SuccGroups)
			errs = err
			continue
		}

		args = append(args, sg)

		fg, err := json.Marshal(e.FailGroups)
		if nil != err {
			log.Error("KiteMysqlStore|batchUpdate|FAIL GROUP|MARSHAL|FAIL|%s|%s|%s\n", err, e.MessageId, e.FailGroups)
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
			log.Error("KiteMysqlStore|batchUpdate|FAIL|%s|%s\n", err, e)
			errs = err
		}

	}
	return nil == errs
}

func (self *KiteMysqlStore) Stop() {
	self.stop = true
	for k, v := range self.stmtPools {
		for _, s := range v {
			s.Shutdown()
		}
		log.Info("KiteMysqlStore|Stop|Stmt|%t\n", k)
	}
}
