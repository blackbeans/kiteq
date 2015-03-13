package main

import (
	"flag"
	"fmt"
	"kiteq/protocol"
	"kiteq/server"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {

	bindHost := flag.String("bind", ":13800", "-bind=localhost:13800")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	topics := flag.String("topics", "", "-topics=trade,a,b")
	db := flag.String("db", "mmap://file:///data/kiteq", "-db=mysql://root:root@tcp(localhost:3306)/kite")
	pprofPort := flag.Int("pport", -1, "pprof port default value is -1 ")

	v := strings.TrimPrefix("mysql://momo:momo1602@tcp(96-1u-a129-001.m6.momo.com:3306)/kit", "mysql://")
	log.Println(v)
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	host, port, _ := net.SplitHostPort(*bindHost)
	go func() {
		if *pprofPort > 0 {
			log.Println(http.ListenAndServe(host+":"+strconv.Itoa(*pprofPort), nil))
		}
	}()

	rc := &protocol.RemotingConfig{
		MaxDispatcherNum: 50,
		MaxWorkerNum:     50000,
		ReadBufferSize:   16 * 1024,
		WriteBufferSize:  16 * 1024,
		WriteChannelSize: 10000,
		ReadChannelSize:  10000,
		IdleTime:         10 * time.Second}

	kc := server.NewKiteQConfig(*bindHost, *zkhost, 100000, 1*time.Minute, strings.Split(*topics, ","), *db, rc)

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
			path := "./heapdump-kiteq-" + host + "_" + port + fmt.Sprintf("%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
			}
		}
	}

	qserver.Shutdown()
	log.Println("KiteQServer IS STOPPED!")
}
