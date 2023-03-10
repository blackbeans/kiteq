package main

import (
	"context"
	"fmt"
	"kiteq/server"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/blackbeans/turbo"
	log "github.com/sirupsen/logrus"
)

func main() {

	//加载启动参数
	so := server.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU()*2 + 1)

	rc := turbo.NewTConfig(
		"remoting",
		1000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second,
		100*10000)

	kc := server.NewKiteQConfig(so, rc)
	ctx, cancel := context.WithCancel(context.Background())
	qserver := server.NewKiteQServer(ctx, kc)
	qserver.Start()

	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGKILL, syscall.SIGUSR1, syscall.SIGTERM)
	//是否收到kill的命令
	for {
		cmd := <-s
		if cmd == syscall.SIGKILL || cmd == syscall.SIGTERM {
			break
		} else if cmd == syscall.SIGUSR1 {
			//如果为siguser1则进行dump内存
			unixtime := time.Now().Unix()
			path := fmt.Sprintf("./heapdump-kiteq-%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
			}
		}
	}
	qserver.Shutdown()
	cancel()
	log.Infof("KiteQServer IS STOPPED!")

}
