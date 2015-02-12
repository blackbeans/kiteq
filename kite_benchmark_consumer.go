package main

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/binding"
	"kiteq/client"
	"kiteq/protocol"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

type defualtListener struct {
	count int32
	lc    int32
}

func (self *defualtListener) monitor() {
	for {
		tmp := self.count
		ftmp := self.lc

		time.Sleep(1 * time.Second)
		fmt.Printf("tps:%d\n", (tmp - ftmp))
		self.lc = ftmp
	}
}

func (self *defualtListener) OnMessage(msg *protocol.StringMessage) bool {
	// log.Println("defualtListener|OnMessage", *msg.Header, *msg.Body)
	atomic.AddInt32(&self.count, 1)
	return true
}

func (self *defualtListener) OnMessageCheck(messageId string, tx *protocol.TxResponse) error {
	// log.Println("defualtListener|OnMessageCheck", messageId)
	tx.Commit()
	return nil
}

func buildStringMessage() *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:   proto.String(messageId()),
		Topic:       proto.String("trade"),
		MessageType: proto.String("pay-succ"),
		ExpiredTime: proto.Int64(13700000000),
		GroupId:     proto.String("go-kite-test"),
		Commit:      proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

var f, _ = os.OpenFile("/dev/urandom", os.O_RDONLY, 0)

func messageId() string {
	b := make([]byte, 16)
	f.Read(b)
	return fmt.Sprintf("%x", b)
}

func main() {
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	flag.Parse()

	lis := &defualtListener{}
	go lis.monitor()

	kite := client.NewKiteQClient(*zkhost, "s-mts-test", "123456", lis)
	kite.SetBindings([]*binding.Binding{
		binding.Bind_Direct("s-mts-test", "trade", "pay-succ", 1000, true),
	})
	kite.Start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Kill)

	select {
	//kill掉的server
	case <-ch:

	}

	kite.Destory()
}
