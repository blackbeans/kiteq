package mysql

import (
	"database/sql"
	"encoding/json"
	log "github.com/blackbeans/log4go"
	. "kiteq/store"
	"time"
)

func (self *KiteMysqlStore) Start() {

	count := self.dbshard.ShardNum() * self.dbshard.HashNum()
	//创建Hash的channel
	batchDelChan := make([]chan string, 0, count)
	batchUpChan := make([]chan *MessageEntity, 0, count)
	batchComChan := make([]chan string, 0, count)
	for i := 0; i < count; i++ {
		batchUpChan = append(batchUpChan, make(chan *MessageEntity, self.batchUpSize))
		batchDelChan = append(batchDelChan, make(chan string, self.batchDelSize))
		batchComChan = append(batchComChan, make(chan string, self.batchUpSize))
	}

	//批量的channel
	self.batchUpChan = batchUpChan
	self.batchDelChan = batchDelChan
	self.batchComChan = batchComChan

	//创建每种批量的preparedstmt
	stmts := make(map[batchType][][]*StmtPool, 4)
	for k, v := range self.sqlwrapper.batchSQL {
		btype := k
		pool := make([][]*StmtPool, 0, self.dbshard.ShardNum())
		//对每个shard构建stmt的pool
		for i := 0; i < self.dbshard.ShardNum(); i++ {
			innerPool := make([]*StmtPool, 0, self.dbshard.HashNum())
			for j, s := range v {
				psql := s
				db := self.dbshard.FindShardById(i*self.dbshard.HashNum() + j).master
				err, p := NewStmtPool(10, 20, 50, 1*time.Minute, func() (error, *sql.Stmt) {
					stmt, err := db.Prepare(psql)
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
				innerPool = append(innerPool, p)
			}
			pool = append(pool, innerPool)
		}
		stmts[btype] = pool
	}

	self.stmtPools = stmts

	for i := 0; i < count; i++ {
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
	idx := self.dbshard.HashId(messageid)
	self.batchComChan[idx] <- messageid
	return true

}

func (self *KiteMysqlStore) AsyncUpdate(entity *MessageEntity) bool {
	idx := self.dbshard.HashId(entity.MessageId)
	self.batchUpChan[idx] <- entity
	return true

}
func (self *KiteMysqlStore) AsyncDelete(messageid string) bool {
	idx := self.dbshard.HashId(messageid)
	self.batchDelChan[idx] <- messageid
	return true
}

func (self *KiteMysqlStore) stmtPool(bt batchType, hash string) *StmtPool {
	shard := self.dbshard.FindForShard(hash)
	id := self.dbshard.FindForKey(hash)
	return self.stmtPools[bt][shard.shardId][id]
}

func (self *KiteMysqlStore) batchCommit(hashId int, messageId []string) bool {

	if len(messageId) <= 0 {
		return true
	}
	// log.Printf("KiteMysqlStore|batchCommit|%s|%s\n", prepareSQL, messageId)
	p := self.stmtPool(COMMIT, messageId[0])
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

	p := self.stmtPool(DELETE, messageId[0])
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

	p := self.stmtPool(UPDATE, entity[0].MessageId)
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
			for _, p := range s {
				p.Shutdown()
			}
		}
		log.Info("KiteMysqlStore|Stop|Stmt|%t\n", k)
	}
	self.dbshard.Stop()
}
