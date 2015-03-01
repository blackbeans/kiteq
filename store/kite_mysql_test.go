package store

import (
	"fmt"
	"strconv"
	"testing"
)

func TestSave(t *testing.T) {
	kiteMysql := NewKiteMysql("root:@/kite")
	kiteMysql.Save(&MessageEntity{
		MessageId:       "0",
		Topic:           "test",
		KiteServer:      "sutao",
		Body:            []byte("abc"),
		NextDeliverTime: 1,
	})

	for i := 0; i < 1000; i++ {
		kiteMysql.Save(&MessageEntity{
			MessageId:       strconv.Itoa(i),
			KiteServer:      "sutao",
			Topic:           fmt.Sprintf("topic %s", i),
			Body:            []byte("abc222:w2"),
			NextDeliverTime: 2,
		})
	}
	ret := kiteMysql.Query("sutao")
	fmt.Println(ret)

	kiteServer := "sutao"
	var nextDeliveryTime int64 = 123
	var startIdx int32 = 0
	var limit int32 = 10
	hashKey := "0"
	fmt.Println("PageQueryEntity")
	isSucess, resultSet := kiteMysql.PageQueryEntity(hashKey, kiteServer, nextDeliveryTime, startIdx, limit)
	if isSucess {
		fmt.Println("result set", resultSet)
	} else {
		fmt.Println("result false")
	}
}

//func Benchmark_Save(b *testing.B) {
//	db := NewKiteMysql("root:root@tcp(localhost:8889)/kite")
//	e := &MessageEntity{
//		Topic: "test",
//		Body:  []byte("abc"),
//	}
//	var f, _ = os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
//	bs := make([]byte, 16)
//
//	for i := 0; i < b.N; i++ {
//		f.Read(bs)
//		e.MessageId = fmt.Sprintf("%x", bs)
//		db.Save(e)
//	}
//	log.Println("finish")
//}

func TestQuery(t *testing.T) {
	// db := NewKiteMysql("root:root@tcp(localhost:8889)/kite")
	// // log.Println(db.Rollback("2"))
	// e := db.Query("2")
	// e.body = []byte("cba")
	// db.UpdateEntity(e)
	// log.Println(db.Query("2"))

	// log.Println(db.Save(&MessageEntity{
	// 	messageId: "2",
	// 	topic:     "test",
	// 	body:      []byte("abc"),
	// }))
}
