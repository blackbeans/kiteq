package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	log "github.com/blackbeans/log4go"
	"github.com/golang/protobuf/proto"
	"io"
	"kiteq/client"
	"kiteq/protocol"
	"kiteq/store"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type defualtListener struct {
}

func (self *defualtListener) OnMessage(msg *protocol.QMessage) bool {
	log.Info("defualtListener|OnMessage", msg.GetHeader(), msg.GetBody())
	return true
}

func (self *defualtListener) OnMessageCheck(tx *protocol.TxResponse) error {
	// log.Info("defualtListener|OnMessageCheck", messageId)
	tx.Commit()
	return nil
}

var body []byte
var rander = rand.Reader // random function
func init() {
	body = make([]byte, 512, 512)
	// randomBits completely fills slice b with random data.
	if _, err := io.ReadFull(rander, body); err != nil {
		panic(err.Error()) // rand should never fail
	}
}

func buildBytesMessage(commit bool) *protocol.BytesMessage {
	//创建消息
	entity := &protocol.BytesMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(store.MessageId()),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(24 * time.Hour).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(commit),
		Fly:          proto.Bool(false)}

	entity.Body = body

	return entity
}

func main() {
	logxml := flag.String("logxml", "./log4go.xml", "-logxml=./log_producer.xml")
	k := flag.Int("k", 1, "-k=1  //kiteclient num ")
	c := flag.Int("c", 10, "-c=100")
	tx := flag.Bool("tx", false, "-tx=true send Tx Message")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	flag.Parse()

	runtime.GOMAXPROCS(8)

	log.LoadConfiguration(*logxml)

	go func() {

		log.Info(http.ListenAndServe(":28000", nil))
	}()

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
	clients := make([]*client.KiteQClient, 0, *k)
	for j := 0; j < *k; j++ {

		kiteClient := client.NewKiteQClient(*zkhost, "pb-mts-test", "123456", &defualtListener{})
		kiteClient.SetTopics([]string{"trade"})
		kiteClient.Start()
		clients = append(clients, kiteClient)
		for i := 0; i < *c; i++ {
			go func(kite *client.KiteQClient) {
				wg.Add(1)
				for !stop {
					if *tx {
						msg := buildBytesMessage(false)
						err := kite.SendTxBytesMessage(msg, doTranscation)
						if nil != err {
							fmt.Printf("SEND TxMESSAGE |FAIL|%s\n", err)
							atomic.AddInt32(&fc, 1)
						} else {
							atomic.AddInt32(&count, 1)
						}
					} else {
						err := kite.SendBytesMessage(buildBytesMessage(true))
						if nil != err {
							// fmt.Printf("SEND MESSAGE |FAIL|%s\n", err)
							atomic.AddInt32(&fc, 1)
						} else {
							atomic.AddInt32(&count, 1)
						}
					}
					// stop = true
				}
				wg.Done()
			}(kiteClient)
		}
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

	for _, k := range clients {
		k.Destory()
	}
}

func doTranscation(message *protocol.QMessage) (bool, error) {
	return true, nil
}
