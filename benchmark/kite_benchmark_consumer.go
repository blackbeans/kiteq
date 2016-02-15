package main

import (
	"flag"
	"fmt"
	log "github.com/blackbeans/log4go"
	"kiteq/binding"
	"kiteq/client"
	"kiteq/protocol"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"syscall"
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
		self.lc = tmp
	}
}

func (self *defualtListener) OnMessage(msg *protocol.QMessage) bool {
	log.Info("defualtListener|OnMessage|%s", msg.GetHeader().GetMessageId())
	atomic.AddInt32(&self.count, 1)
	return true
}

func (self *defualtListener) OnMessageCheck(tx *protocol.TxResponse) error {
	log.Info("defualtListener|OnMessageCheck", tx.MessageId)
	tx.Commit()
	return nil
}

func main() {
	logxml := flag.String("logxml", "../log/log_consumer.xml", "-logxml=../log/log_consumer.xml")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	flag.Parse()
	runtime.GOMAXPROCS(8)

	log.LoadConfiguration(*logxml)
	go func() {

		log.Info(http.ListenAndServe(":38000", nil))
	}()

	lis := &defualtListener{}
	go lis.monitor()

	kite := client.NewKiteQClient(*zkhost, "s-mts-test", "123456", lis)
	kite.SetBindings([]*binding.Binding{
		binding.Bind_Direct("s-mts-test", "relation", "pay-succ", 1000, true),
	})
	kite.Start()

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
			path := "./heapdump-consumer" + fmt.Sprintf("%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
			}
		}
	}
	kite.Destory()
}
