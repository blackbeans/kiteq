package main

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/client"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

type defualtListener struct {
}

func (self *defualtListener) OnMessage(msg *protocol.StringMessage) bool {
	log.Println("defualtListener|OnMessage", *msg.Header, *msg.Body)
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
		MessageId:    proto.String(store.MessageId()),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true)}
	entity.Body = proto.String("echo")

	return entity
}

func main() {

	c := flag.Int("c", 10, "-c=10")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	flag.Parse()

	kite := client.NewKiteQClient(*zkhost, "pb-mts-test", "123456", &defualtListener{})
	kite.SetTopics([]string{"trade"})
	kite.Start()

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

	wg := &sync.WaitGroup{}

	stop := false
	for i := 0; i < *c; i++ {
		go func() {
			wg.Add(1)
			for !stop {
				err := kite.SendStringMessage(buildStringMessage())
				if nil != err {
					fmt.Printf("SEND MESSAGE |FAIL|%s\n", err)
					atomic.AddInt32(&fc, 1)
				} else {
					atomic.AddInt32(&count, 1)
				}
			}
			wg.Done()
		}()
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Kill)

	select {
	//kill掉的server
	case <-ch:
		stop = true
	}

	wg.Wait()
	kite.Destory()
}
