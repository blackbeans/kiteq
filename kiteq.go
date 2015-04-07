package main

import (
	"flag"
	"fmt"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"kiteq/server"
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
	logxml := flag.String("logxml", "./log/log.xml", "-logxml=./log/log.xml")
	bindHost := flag.String("bind", ":13800", "-bind=localhost:13800")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	topics := flag.String("topics", "", "-topics=trade,a,b")
	db := flag.String("db", "memory://initcap=100000&maxcap=200000",
		"-db=mysql://master:3306,slave:3306?db=kite&username=root&password=root&maxConn=500&batchUpdateSize=1000&batchDelSize=1000&flushPeriod=1000")
	pprofPort := flag.Int("pport", -1, "pprof port default value is -1 ")

	flag.Parse()

	//加载log4go的配置
	log.LoadConfiguration(*logxml)

	runtime.GOMAXPROCS(runtime.NumCPU())

	host, port, _ := net.SplitHostPort(*bindHost)
	go func() {
		if *pprofPort > 0 {
			log.Error(http.ListenAndServe(host+":"+strconv.Itoa(*pprofPort), nil))
		}
	}()

	rc := turbo.NewRemotingConfig(
		"remoting-"+*bindHost,
		2000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	kc := server.NewKiteQConfig("kiteq-"+*bindHost, *bindHost, *zkhost, 1*time.Second, 8000, 5*time.Second, strings.Split(*topics, ","), *db, rc)

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
	log.Info("KiteQServer IS STOPPED!")

}
