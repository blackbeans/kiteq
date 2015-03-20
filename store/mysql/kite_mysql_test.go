package mysql

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPageQuery(t *testing.T) {

	options := MysqlOptions{
		Addr:         "root:@tcp(localhost:3306)/kite",
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options)
	truncate(kiteMysql)
	hn, _ := os.Hostname()
	for i := 0; i < 10; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(strconv.Itoa(i) + "26c03f00665862591f696a980b5a6c4"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))

		entity.KiteServer = hn
		entity.PublishTime = time.Now().Unix()
		kiteMysql.Save(entity)
	}

	startIdx := 0
	hasMore := true
	count := 0
	//开始分页查询未过期的消息实体
	for hasMore {
		more, entities := kiteMysql.PageQueryEntity("4", hn,
			time.Now().Unix(), 0, 1)
		if len(entities) <= 0 {
			break
		}

		//开始发起重投
		for _, entity := range entities {
			count++
			t.Logf("TestPageQuery|PageQueryEntity|%s\n", entity.MessageId)
			msg := &store.MessageEntity{
				MessageId:    entity.MessageId,
				DeliverCount: 1,
				SuccGroups:   []string{},
				FailGroups:   []string{"s-mts-test"},
				//设置一下下一次投递时间
				NextDeliverTime: time.Now().Add(1 * time.Minute).Unix()}
			kiteMysql.AsyncUpdate(msg)

		}

		time.Sleep(1 * time.Second)
		hasMore = more
		startIdx += len(entities)
	}
	if count != 10 {
		t.Fail()
		t.Logf("TestPageQuery|IDX|FAIL|%d\n", count)
		return
	}

	startIdx = 0
	hasMore = true
	//开始分页查询未过期的消息实体
	for hasMore {
		more, entities := kiteMysql.PageQueryEntity("4", hn,
			time.Now().Add(8*time.Minute).Unix(), startIdx, 1)
		if len(entities) <= 0 {
			t.Logf("TestPageQuery|CHECK|NO DATA|%s\n", entities)
			break
		}

		//开始发起重投
		for _, entity := range entities {
			if entity.DeliverCount != 1 || entity.FailGroups[0] != "s-mts-test" {
				t.Fail()
			}
			t.Logf("TestPageQuery|PageQueryEntity|CHECK|%s\n", entity.MessageId)
		}
		startIdx += len(entities)
		hasMore = more
	}

	t.Logf("TestPageQuery|CHECK|%d\n", startIdx)
	if startIdx != 10 {
		t.Fail()
	}

	// truncate(kiteMysql)

}

func TestBatch(t *testing.T) {

	options := MysqlOptions{
		Addr:         "root:@tcp(localhost:3306)/kite",
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options)

	truncate(kiteMysql)

	mids := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5a6c4" + strconv.Itoa(i)),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))
		entity.SuccGroups = []string{"go-kite-test"}
		hn, _ := os.Hostname()
		entity.KiteServer = hn
		entity.PublishTime = time.Now().Unix()
		kiteMysql.Save(entity)
		mids = append(mids, entity.MessageId)
	}

	for _, v := range mids {

		msg := &store.MessageEntity{
			MessageId:    v,
			DeliverCount: 1,
			SuccGroups:   []string{"s-mts-test"},
			FailGroups:   []string{},
			//设置一下下一次投递时间
			NextDeliverTime: time.Now().Unix()}
		kiteMysql.AsyncUpdate(msg)
	}

	time.Sleep(5 * time.Second)
	for _, v := range mids {
		e := kiteMysql.Query(v)
		if nil == e || len(e.SuccGroups) < 1 {
			t.Fatalf("TestBatch|Update FAIL|%s\n", e)
			t.Fail()
			return
		}
		t.Logf("Query|%s\n", e)
	}

	//测试批量删除
	for _, v := range mids {
		kiteMysql.AsyncDelete(v)
	}
	time.Sleep(5 * time.Second)
	for _, v := range mids {
		entity := kiteMysql.Query(v)
		if nil != entity {
			t.Fatalf("TestBatch|AysncDelete FAIL|%s\n", entity)
			t.Fail()

		}
	}

	truncate(kiteMysql)
}

func truncate(k *KiteMysqlStore) {
	for i := 0; i < 16; i++ {
		_, err := k.db.Exec(fmt.Sprintf("truncate table kite_msg_%d", i))
		if nil != err {
			log.Printf("truncate table kite_msg_%d|%s\n", i, err)
			return
		}
	}
}

func TestStringSave(t *testing.T) {

	//创建消息
	msg := &protocol.StringMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5a6c40"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}

	msg.Body = proto.String("hello world")
	innerT(msg, "26c03f00665862591f696a980b5a6c40", t)
}

func TestBytesSave(t *testing.T) {

	//创建消息
	msg := &protocol.BytesMessage{}
	msg.Header = &protocol.Header{
		MessageId:    proto.String("26c03f00665862591f696a980b5a6c41"),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(false),
		Fly:          proto.Bool(false)}

	msg.Body = []byte("hello world")
	innerT(msg, "26c03f00665862591f696a980b5a6c41", t)
}

func innerT(msg interface{}, msgid string, t *testing.T) {
	qm := protocol.NewQMessage(msg)
	entity := store.NewMessageEntity(qm)
	entity.SuccGroups = []string{"go-kite-test"}
	hn, _ := os.Hostname()
	entity.KiteServer = hn
	entity.PublishTime = time.Now().Unix()

	options := MysqlOptions{
		Addr:         "root:@tcp(localhost:3306)/kite",
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options)

	truncate(kiteMysql)

	succ := kiteMysql.Save(entity)
	if !succ {
		t.Fail()
	} else {
		t.Logf("SAVE|SUCC|%s\n", entity)
	}

	ret := kiteMysql.Query(msgid)
	t.Logf("Query|%s\n", ret)
	bb, ok := qm.GetBody().([]byte)
	if ok {
		rb, _ := ret.GetBody().([]byte)
		if string(rb) != string(bb) {
			t.Fail()
		} else {
			t.Logf("Query|SUCC|%s\n", ret)
		}
	} else {
		bs, _ := qm.GetBody().(string)
		rs, _ := ret.GetBody().(string)
		if bs != rs {
			t.Fail()
		} else {
			t.Logf("Query|SUCC|%s\n", ret)
		}
	}

	fmt.Println("Commint BEGIN")
	kiteMysql.Commit(msgid)
	fmt.Println("Commint END")

	ret = kiteMysql.Query(msgid)
	if !ret.Commit {
		t.Logf("Commit|FAIL|%s\n", ret)
		t.Fail()
	}

	hasNext, entities := kiteMysql.PageQueryEntity(msgid, hn, time.Now().Unix(), 0, 10)
	t.Logf("PageQueryEntity|%s\n", entities)
	if hasNext {
		t.Logf("PageQueryEntity|FAIL|HasNext|%s\n", entities)
		t.Fail()
	} else {
		if len(entities) != 1 {
			t.Logf("PageQueryEntity|FAIL|%s\n", entities)
			t.Fail()
		} else {
			if entities[0].Header.GetMessageId() != qm.GetHeader().GetMessageId() {
				t.Fail()
			}
		}
	}

}
