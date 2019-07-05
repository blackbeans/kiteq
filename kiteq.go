package main

import (
	"fmt"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"kiteq/server"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"
)

func main() {

	//加载启动参数
	so := server.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	rc := turbo.NewTConfig(
		"remoting",
		1000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second,
		100*10000)

	kc := server.NewKiteQConfig(so, rc)

	qserver := server.NewKiteQServer(kc)
	qserver.Start()

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
	log.InfoLog("kite_server", "KiteQServer IS STOPPED!")

}
