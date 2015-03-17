package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"kiteq/protocol"
	. "kiteq/store"
	"log"
	"time"
)

//mysql的参数
type MysqlOptions struct {
	Addr                      string
	BatchUpSize, BatchDelSize int
	FlushPeriod               time.Duration
	MaxIdleConn               int
	MaxOpenConn               int
}

type KiteMysqlStore struct {
	convertor    convertor
	sqlwrapper   *sqlwrapper
	db           *sql.DB
	batchUpChan  []chan *MessageEntity
	batchDelChan []chan string
	batchUpSize  int
	batchDelSize int
	flushPeriod  time.Duration
}

func NewKiteMysql(options MysqlOptions) *KiteMysqlStore {

	db, err := sql.Open("mysql", options.Addr)
	if err != nil {
		log.Panicf("NewKiteMysql|CONNECT FAIL|%s|%s\n", err, options.Addr)
	}

	db.SetMaxIdleConns(options.MaxIdleConn)
	db.SetMaxOpenConns(options.MaxOpenConn)

	sqlwrapper := newSqlwrapper("kite_msg", HashShard{}, MessageEntity{})

	//创建Hash的channel
	batchDelChan := make([]chan string, 0, sqlwrapper.hashshard.ShardCnt())
	batchUpChan := make([]chan *MessageEntity, 0, sqlwrapper.hashshard.ShardCnt())
	for i := 0; i < sqlwrapper.hashshard.ShardCnt(); i++ {
		batchUpChan = append(batchUpChan, make(chan *MessageEntity, options.BatchUpSize/sqlwrapper.hashshard.ShardCnt()))
		batchDelChan = append(batchDelChan, make(chan string, options.BatchDelSize/sqlwrapper.hashshard.ShardCnt()))
	}

	ins := &KiteMysqlStore{
		db:           db,
		convertor:    convertor{columns: sqlwrapper.columns},
		sqlwrapper:   sqlwrapper,
		batchUpChan:  batchUpChan,
		batchUpSize:  cap(batchDelChan),
		batchDelChan: batchDelChan,
		batchDelSize: cap(batchDelChan),
		flushPeriod:  options.FlushPeriod}

	log.Printf("NewKiteMysql|KiteMysqlStore|SUCC|%s...\n", options.Addr)
	ins.start()
	return ins
}

func (self *KiteMysqlStore) Query(messageId string) *MessageEntity {

	s := self.sqlwrapper.hashQuerySQL(messageId)
	rows, err := self.db.Query(s, messageId)
	if err != nil {
		log.Printf("KiteMysqlStore|Query|FAIL|%s|%s\n", err, messageId)
		return nil
	}
	defer rows.Close()

	var entity *MessageEntity

	if rows.Next() {
		fc := make([]interface{}, 0, len(self.sqlwrapper.columns))
		err := rows.Scan(fc...)
		if nil != err {
			log.Printf("KiteMysqlStore|Query|FAIL|%s|%s\n", err, messageId)
			return nil
		} else {
			entity = self.convertor.Convert2Entity(fc)
			switch entity.MsgType {
			case protocol.CMD_BYTES_MESSAGE:
				//do nothing
			case protocol.CMD_STRING_MESSAGE:
				entity.Body = string(entity.GetBody().([]byte))
			}
		}
	}
	return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	fvs := self.convertor.Convert2Params(entity)
	s := self.sqlwrapper.hashSaveSQL(entity.MessageId)
	result, err := self.db.Exec(s, fvs...)
	if err != nil {
		log.Printf("KiteMysqlStore|SAVE|FAIL|%s|%s\n", err, entity.MessageId)
		return false
	}

	num, _ := result.RowsAffected()

	return num == 1
}

func (self *KiteMysqlStore) Commit(messageId string) bool {

	s := self.sqlwrapper.hashCommitSQL(messageId)
	result, err := self.db.Exec(s, messageId)
	if err != nil {
		log.Printf("KiteMysqlStore|Commit|FAIL|%s|%s\n", err, messageId)
		return false
	}
	num, _ := result.RowsAffected()
	return num == 1
}

func (self *KiteMysqlStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}

func (self *KiteMysqlStore) Delete(messageId string) bool {

	s := self.sqlwrapper.hashDeleteSQL(messageId)
	result, err := self.db.Exec(s, messageId)
	if err != nil {
		log.Printf("KiteMysqlStore|Delete|FAIL|%s|%s\n", err, messageId)
		return false
	}
	num, _ := result.RowsAffected()
	return num == 1
}

//没有body的entity
func (self *KiteMysqlStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int32) (bool, []*MessageEntity) {

	s := self.sqlwrapper.hashPQSQL(hashKey)

	rows, err := self.db.Query(s, kiteServer, nextDeliveryTime, startIdx, limit+1)
	if err != nil {
		log.Printf("KiteMysqlStore|Query|FAIL|%s|%s\n", err, hashKey)
		return false, nil
	}
	defer rows.Close()

	results := make([]*MessageEntity, 0, limit)
	for rows.Next() {
		fc := make([]interface{}, 0, len(self.sqlwrapper.columns))
		err := rows.Scan(fc...)
		if err != nil {
			log.Printf("KiteMysqlStore|PageQueryEntity|FAIL|%s|%s|%d|%d\n", err, kiteServer, nextDeliveryTime, startIdx)
		} else {
			entity := self.convertor.Convert2Entity(fc)
			if nil != entity {
				results = append(results, entity)
			}
		}
	}

	if len(results) > int(limit) {
		return true, results[:limit]
	} else {
		return false, results
	}
}
