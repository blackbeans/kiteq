package mysql

import (
	"context"
	"database/sql"
	"fmt"
	. "kiteq/store"
	"strings"
	"sync"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
)

//mysql的参数
type MysqlOptions struct {
	ShardNum                  int //分库的数量
	Addr                      string
	SlaveAddr                 string
	DB                        string
	Username, Password        string
	BatchUpSize, BatchDelSize int
	FlushPeriod               time.Duration
	MaxIdleConn               int
	MaxOpenConn               int
}

type KiteMysqlStore struct {
	convertor    convertor
	sqlwrapper   *sqlwrapper
	dbshard      DbShard
	batchUpChan  []chan *MessageEntity
	batchDelChan []chan string
	batchComChan []chan string
	batchUpSize  int
	batchDelSize int
	flushPeriod  time.Duration
	stmtPools    map[batchType][][]*sql.Stmt //第一层dblevel 第二维table level
	stop         bool
	serverName   string
}

func NewKiteMysql(ctx context.Context, options MysqlOptions, serverName string) *KiteMysqlStore {

	shard := newDbShard(options)

	sqlwrapper := newSqlwrapper("kite_msg", shard, MessageEntity{})

	ins := &KiteMysqlStore{
		dbshard:      shard,
		convertor:    convertor{columns: sqlwrapper.columns},
		sqlwrapper:   sqlwrapper,
		batchUpSize:  options.BatchUpSize,
		batchDelSize: options.BatchDelSize,
		flushPeriod:  options.FlushPeriod,
		serverName:   serverName,
		stop:         false}

	log.InfoLog("kite_store", "NewKiteMysql|KiteMysqlStore|SUCC|%s|%s...\n", options.Addr, options.SlaveAddr)
	return ins
}

func (self *KiteMysqlStore) RecoverNum() int {
	return self.dbshard.ShardNum() * self.dbshard.HashNum()
}

var filternothing = func(colname string) bool {
	return false
}

func (self *KiteMysqlStore) Length() map[string] /*topic*/ int {
	//TODO mysql中的未过期的消息数量
	defer func() {
		if err := recover(); nil != err {

		}
	}()
	stat := make(map[string]int, 10)
	//开始查询Mysql中的堆积消息数量
	for i := 0; i < self.RecoverNum(); i++ {
		func() error {
			hashKey := fmt.Sprintf("%x", i)
			s := self.sqlwrapper.hashMessageStatSQL(hashKey)
			// log.Println(s)
			rows, err := self.dbshard.FindSlave(hashKey).Query(s, self.serverName, time.Now().Unix())
			if err != nil {
				log.ErrorLog("kite_store", "KiteMysqlStore|Length|Query|FAIL|%s|%s|%s", err, hashKey, s)
				return err
			}
			defer rows.Close()
			if rows.Next() {
				topic := ""
				num := 0
				err = rows.Scan(&topic, &num)
				if nil != err {
					log.ErrorLog("kite_store", "KiteMysqlStore|Length|Scan|FAIL|%s|%s|%s\n", err, hashKey, s)
					return err
				} else {
					v, ok := stat[topic]
					if !ok {
						v = 0
					}
					stat[topic] = v + num
				}
			}
			return nil
		}()
	}

	return stat
}

func (self *KiteMysqlStore) Monitor() string {
	line := "KiteMysqlStore:\t"
	for _, r := range self.dbshard.shardranges {
		line += fmt.Sprintf("[master:%d,slave:%d]@[%d,%d]\t", r.master.Stats().OpenConnections,
			r.slave.Stats().OpenConnections, r.min, r.max)
	}
	return line
}

func (self *KiteMysqlStore) Query(topic, messageId string) *MessageEntity {

	var entity *MessageEntity
	s := self.sqlwrapper.hashQuerySQL(messageId)
	rows, err := self.dbshard.FindSlave(messageId).Query(s, messageId)
	if nil != err {
		log.ErrorLog("kite_store", "KiteMysqlStore|Query|FAIL|%s|%s\n", err, messageId)
		return nil
	}
	defer rows.Close()

	if rows.Next() {

		entity = &MessageEntity{}
		fc := self.convertor.convertFields(entity, filternothing)
		err := rows.Scan(fc...)
		if nil != err {
			log.ErrorLog("kite_store", "KiteMysqlStore|Query|SCAN|FAIL|%s|%s\n", err, messageId)
			return nil
		}
		self.convertor.Convert2Entity(fc, entity, filternothing)
		switch entity.MsgType {
		case protocol.CMD_BYTES_MESSAGE:
			//do nothing
		case protocol.CMD_STRING_MESSAGE:
			entity.Body = string(entity.GetBody().([]byte))
		}
	}

	return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	fvs := self.convertor.Convert2Params(entity)
	s := self.sqlwrapper.hashSaveSQL(entity.MessageId)
	result, err := self.dbshard.FindMaster(entity.MessageId).Exec(s, fvs...)
	if err != nil {
		log.ErrorLog("kite_store", "KiteMysqlStore|SAVE|FAIL|%s|%s\n", err, entity.MessageId)
		return false
	}

	num, _ := result.RowsAffected()
	return num == 1
}

func (self *KiteMysqlStore) Commit(topic, messageId string) bool {
	return self.AsyncCommit(topic, messageId)
}

func (self *KiteMysqlStore) Rollback(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *KiteMysqlStore) Delete(topic, messageId string) bool {
	return self.AsyncDelete(topic, messageId)
}

func (self *KiteMysqlStore) Expired(topic, messageId string) bool { return true }

func (self *KiteMysqlStore) MoveExpired() {
	wg := sync.WaitGroup{}

	now := time.Now().Unix()
	//开始查询Mysql中的堆积消息数量
	for i := 0; i < self.RecoverNum(); i++ {
		hashKey := fmt.Sprintf("%x", i)
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			self.migrateMessage(now, hashKey)
		}()
	}
	wg.Wait()
}

//迁移过期的消息
func (self *KiteMysqlStore) migrateMessage(now int64, hashKey string) {

	log.InfoLog("kite_store", "KiteMysqlStore|MoveExpired|START|%s|%d", hashKey)

	//需要将过期的消息迁移到DLQ中
	sql := self.sqlwrapper.hashDLQSQL(DLQ_MOVE_QUERY, hashKey)
	//获取到需要导入的id，然后导入
	isql := self.sqlwrapper.hashDLQSQL(DLQ_MOVE_INSERT, hashKey)
	//删除已导入的数据
	dsql := self.sqlwrapper.hashDLQSQL(DLQ_MOVE_DELETE, hashKey)

	start := 0
	limit := 50
	for {
		messageIds := make([]interface{}, 1, 50)
		err := func() error {
			rows, err := self.dbshard.FindSlave(hashKey).Query(sql, self.serverName, now, start, limit)
			if err != nil {
				log.ErrorLog("kite_store", "KiteMysqlStore|migrateMessage|Query|FAIL|%s|%s|%s", err, hashKey, sql)
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int
				var messageId string
				err = rows.Scan(&id, &messageId)
				if nil != err {
					log.ErrorLog("kite_store", "KiteMysqlStore|MoveExpired|Scan|FAIL|%s|%s|%s", err, hashKey, sql)
				} else {
					start = id
					messageIds = append(messageIds, messageId)
				}
			}
			return nil
		}()

		//已经搬迁完毕则退出进行下一个
		if nil != err || len(messageIds[1:]) <= 0 {
			log.WarnLog("kite_store", "KiteMysqlStore|MoveExpired|SUCC|%s|%d|%s", hashKey, start, err)
			break
		}

		in := strings.Repeat("?,", len(messageIds[1:]))
		in = in[:len(in)-1]
		isqlTmp := strings.Replace(isql, "{ids}", in, 1)
		_, err = self.dbshard.FindMaster(hashKey).Exec(isqlTmp, messageIds[1:]...)
		if err != nil {
			log.ErrorLog("kite_store", "KiteMysqlStore|MoveExpired|Insert|FAIL|%s|%s", err, isqlTmp, messageIds)
			break
		}

		dsqlTmp := strings.Replace(dsql, "{ids}", in, 1)
		messageIds[0] = self.serverName
		_, err = self.dbshard.FindMaster(hashKey).Exec(dsqlTmp, messageIds...)
		if err != nil {
			log.ErrorLog("kite_store", "KiteMysqlStore|MoveExpired|DELETE|FAIL|%s|%s|%s|%s", err, dsql, dsqlTmp, messageIds)
			break
		}
	}
}

var filterbody = func(colname string) bool {
	//不需要查询body
	return colname == "body"
}

//没有body的entity
func (self *KiteMysqlStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	s := self.sqlwrapper.hashPQSQL(hashKey)
	// log.Println(s)
	rows, err := self.dbshard.FindSlave(hashKey).
		Query(s, kiteServer, time.Now().Unix(), nextDeliveryTime, startIdx, limit+1)
	if err != nil {
		log.ErrorLog("kite_store", "KiteMysqlStore|Query|FAIL|%s|%s\n", err, hashKey)
		return false, nil
	}
	defer rows.Close()

	results := make([]*MessageEntity, 0, limit)
	for rows.Next() {

		entity := &MessageEntity{}
		fc := self.convertor.convertFields(entity, filterbody)
		err := rows.Scan(fc...)
		if err != nil {
			log.ErrorLog("kite_store", "KiteMysqlStore|PageQueryEntity|FAIL|%s|%s|%d|%d\n", err, kiteServer, nextDeliveryTime, startIdx)
		} else {

			self.convertor.Convert2Entity(fc, entity, filterbody)
			results = append(results, entity)
		}
	}

	if len(results) > limit {
		return true, results[:limit]
	} else {
		return false, results
	}
}
