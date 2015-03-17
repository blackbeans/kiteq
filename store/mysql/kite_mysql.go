package mysql

import (
	"database/sql"
	"encoding/json"
	"github.com/blackbeans/gorp"
	_ "github.com/go-sql-driver/mysql"
	"kiteq/protocol"
	. "kiteq/store"
	"log"
	"time"
)

type KiteMysqlStore struct {
	addr         string
	dbmap        *gorp.DbMap
	batchUpChan  []chan *MessageEntity
	batchDelChan []chan string
	batchUpSize  int
	batchDelSize int
	hashshard    *HashShard
	flushPeriod  time.Duration
}

func NewKiteMysql(addr string, batchUpSize, batchDelSize int, flushPeriod time.Duration) *KiteMysqlStore {
	db, err := sql.Open("mysql", addr)
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(1024)
	if err != nil {
		log.Panicf("NewKiteMysql|CONNECT FAIL|%s|%s\n", err, addr)
	}

	hashshard := &HashShard{}
	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	// add a table, setting the table name and
	// specifying that the Id property is an auto incrementing PK
	dbmap.AddTableWithName(MessageEntity{}, "kite_msg").
		SetKeys(false, "MessageId").
		SetHashKey("MessageId").
		SetShardStrategy(hashshard)

	dbmap.TypeConverter = Convertor{}

	// create the table. in a production system you'd generally
	// use a migration tool, or create the tables via scripts
	err = dbmap.CreateTablesIfNotExists()
	if err != nil {
		log.Panicf("NewKiteMysql|CreateTablesIfNotExists|FAIL|%s\n", err)
	}

	//创建Hash的channel
	batchDelChan := make([]chan string, 0, hashshard.ShardCnt())
	batchUpChan := make([]chan *MessageEntity, 0, hashshard.ShardCnt())
	for i := 0; i < hashshard.ShardCnt(); i++ {
		batchUpChan = append(batchUpChan, make(chan *MessageEntity, batchDelSize/hashshard.ShardCnt()))
		batchDelChan = append(batchDelChan, make(chan string, batchDelSize/hashshard.ShardCnt()))
	}

	ins := &KiteMysqlStore{
		addr:         addr,
		dbmap:        dbmap,
		hashshard:    hashshard,
		batchUpChan:  batchUpChan,
		batchUpSize:  batchUpSize / hashshard.ShardCnt(),
		batchDelChan: batchDelChan,
		batchDelSize: batchDelSize / hashshard.ShardCnt(),
		flushPeriod:  flushPeriod}

	log.Printf("NewKiteMysql|KiteMysqlStore|SUCC|%s...\n", addr)
	ins.start()
	return ins
}

func (self *KiteMysqlStore) Query(messageId string) *MessageEntity {
	var e MessageEntity
	obj, err := self.dbmap.Get(&e, messageId, messageId)
	if err != nil {
		log.Printf("KiteMysqlStore|Query|FAIL|%s|%s\n", err, messageId)
		return nil
	}
	if obj == nil {
		return nil
	}
	entity := obj.(*MessageEntity)
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		//do nothing
	case protocol.CMD_STRING_MESSAGE:
		entity.Body = string(entity.GetBody().([]byte))
	}
	return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	err := self.dbmap.Insert(entity)
	if err != nil {
		log.Printf("KiteMysqlStore|Save|FAIL|%s\n", err)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Commit(messageId string) bool {
	values := make(map[string]interface{}, 1)
	values["Commit"] = true
	cond := make([]gorp.Cond, 1)
	cond[0] = gorp.Cond{
		Field:    "MessageId",
		Value:    messageId,
		Operator: "=",
	}
	updateCond := gorp.UpdateCond{
		Values: values,
		Cond:   cond,
		Ptr:    &MessageEntity{},
	}
	_, err := self.dbmap.UpdateByColumn(messageId, messageId, updateCond)
	if err != nil {
		log.Printf("KiteMysqlStore|Commit|FAIL|%s|%s\n", err, messageId)
		return false
	}
	return true
}

func (self *KiteMysqlStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}

func (self *KiteMysqlStore) Delete(messageId string) bool {
	entity := &MessageEntity{MessageId: messageId}
	_, err := self.dbmap.Delete(entity)
	if err != nil {
		log.Printf("KiteMysqlStore|Delete|FAIL|%s|%s\n", err, messageId)
		return false
	}

	return true
}

func (self *KiteMysqlStore) UpdateEntity(entity *MessageEntity) bool {

	values := make(map[string]interface{}, 1)
	values["MessageId"] = entity.MessageId
	values["DeliverCount"] = entity.DeliverCount

	sg, err := json.Marshal(entity.SuccGroups)
	if nil == err {
		values["SuccGroups"] = string(sg)
	} else {
		log.Printf("KiteMysqlStore|UpdateEntity|SUCC GROUP|MARSHAL|FAIL|%s|%s|%s\n", err, entity.MessageId, entity.SuccGroups)
		return false
	}

	fg, err := json.Marshal(entity.FailGroups)
	if nil == err {
		values["FailGroups"] = string(fg)
	} else {
		log.Printf("KiteMysqlStore|UpdateEntity|FAIL GROUP|MARSHAL|FAIL|%s|%s|%s\n", err, entity.MessageId, entity.FailGroups)
		return false
	}

	//设置一下下一次投递时间
	values["NextDeliverTime"] = entity.NextDeliverTime

	cond := make([]gorp.Cond, 1)
	cond[0] = gorp.Cond{
		Field:    "MessageId",
		Value:    entity.MessageId,
		Operator: "=",
	}

	updateCond := gorp.UpdateCond{
		Values: values,
		Cond:   cond,
		Ptr:    entity,
	}

	_, err = self.dbmap.UpdateByColumn(entity.MessageId, entity.MessageId, updateCond)
	if err != nil {
		log.Printf("KiteMysqlStore|UpdateEntity|FAIL|%s|%s\n", err, entity)
		return false
	}

	return true
}

//没有body的entity
func (self *KiteMysqlStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int32) (bool, []*MessageEntity) {
	cond := make([]*gorp.Cond, 2)
	cond[0] = &gorp.Cond{Field: "KiteServer", Operator: "=", Value: kiteServer}
	cond[1] = &gorp.Cond{Field: "NextDeliverTime", Operator: "<=", Value: nextDeliveryTime}

	rawResults, err := self.dbmap.BatchQuery(hashKey, startIdx, limit+1, MessageEntity{}, cond, func(col string) bool {
		return col == "body"
	})

	if err != nil {
		log.Printf("KiteMysqlStore|PageQueryEntity|FAIL|%s|%s|%d|%d\n", err, kiteServer, nextDeliveryTime, startIdx)
		return false, nil
	}

	results := make([]*MessageEntity, 0, len(rawResults))
	for _, v := range rawResults {
		entity := v.(*MessageEntity)
		switch entity.MsgType {
		case protocol.CMD_BYTES_MESSAGE:
			//do nothing
		case protocol.CMD_STRING_MESSAGE:
			entity.Body = string(entity.GetBody().([]byte))
		}
		results = append(results, entity)
	}

	if len(results) > int(limit) {
		return true, results[:limit]
	} else {
		return false, results
	}
}
