package main

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/client"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type defualtListener struct {
}

func (self *defualtListener) OnMessage(msg *protocol.QMessage) bool {
	log.Println("defualtListener|OnMessage", msg.GetHeader(), msg.GetBody())
	return true
}

func (self *defualtListener) OnMessageCheck(messageId string, tx *protocol.TxResponse) error {
	// log.Println("defualtListener|OnMessageCheck", messageId)
	tx.Commit()
	return nil
}

func buildStringMessage(commit bool) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(store.MessageId()),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(commit)}
	entity.Body = proto.String("echo")

	return entity
}

func main() {
	c := flag.Int("c", 10, "-c=10")
	tx := flag.Bool("tx", false, "-tx=true send Tx Message")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	flag.Parse()

	go func() {

		log.Println(http.ListenAndServe(":28000", nil))
	}()

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
				if *tx {
					msg := buildStringMessage(false)
					err := kite.SendTxStringMessage(msg, doTranscation)
					if nil != err {
						fmt.Printf("SEND TxMESSAGE |FAIL|%s\n", err)
						atomic.AddInt32(&fc, 1)
					} else {
						atomic.AddInt32(&count, 1)
					}
				} else {
					err := kite.SendStringMessage(buildStringMessage(true))
					if nil != err {
						fmt.Printf("SEND MESSAGE |FAIL|%s\n", err)
						atomic.AddInt32(&fc, 1)
					} else {
						atomic.AddInt32(&count, 1)
					}
				}

			}
			wg.Done()
		}()
	}

	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGKILL, syscall.SIGUSR1)
	//是否收到kill的命令
	for {
		cmd := <-s
		if cmd == syscall.SIGKILL {
			break
		} else if cmd == syscall.SIGUSR1 {
			//如果为siguser1则进行dump内存
			unixtime := time.Now().Unix()
			path := "./heapdump-producer" + fmt.Sprintf("%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
			}
		}
	}

	wg.Wait()
	kite.Destory()
}

func doTranscation(message *protocol.QMessage) (bool, error) {
	return true, nil
}
