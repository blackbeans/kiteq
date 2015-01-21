package main

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"go-kite/client"
	"go-kite/protocol"
	"math/rand"
	"sync/atomic"
	"time"
)

func buildStringMessage() *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:   proto.String("1234567"),
		Topic:       proto.String("trade"),
		MessageType: proto.String("pay-succ"),
		ExpiredTime: proto.Int64(13700000000),
		GroupId:     proto.String("go-kite-test"),
		Commited:    proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

func main() {

	c := flag.Int("c", 10, "-c=10")
	conn := flag.Int("conn", 1, "-conn=1")
	local := flag.String("local", "localhost:13800", "-local=localhost:13800")
	remote := flag.String("remote", "localhost:13800", "-remote=localhost:13800")
	flag.Parse()
	clients := make([]*client.KiteClient, 0, *conn)
	for i := 0; i < *conn; i++ {
		//开始向服务端发送数据
		kclient := client.NewKitClient(*local, *remote, "/user-service", "123456")
		clients = append(clients, kclient)
	}

	count := int32(0)
	lc := int32(0)

	fc := int32(0)
	flc := int32(0)

	go func() {
		for {

			tmp := count
			ftmp := fc

			time.Sleep(1 * time.Second)
			fmt.Printf("tps:%d/%d\n", (tmp - lc), (ftmp - flc))
			lc = tmp
			flc = ftmp
		}
	}()

	for j := 0; j < *conn; j++ {
		for i := 0; i < *c; i++ {
			go func() {
				for {
					idx := rand.Intn(len(clients))
					tmpclient := clients[idx]
					err := tmpclient.SendMessage(buildStringMessage())
					if nil != err {
						fmt.Printf("SEND MESSAGE |FAIL|%s\n", err)
						atomic.AddInt32(&fc, 1)
					} else {
						atomic.AddInt32(&count, 1)
					}
				}

			}()
		}
	}

}
