package main

import (
	"flag"
	"kiteq/server"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
)

func main() {

	bindHost := flag.String("bind", ":13800", "-bind=localhost:13800")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	topics := flag.String("topics", "", "-topics=trade,a,b")
	mysql := flag.String("mysql", "", "-mysql=root:root@tcp(localhost:3306)/kite")
	pprofPort := flag.Int("pport", -1, "pprof port default value is -1 ")

	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	host, _, _ := net.SplitHostPort(*bindHost)
	go func() {
		if *pprofPort > 0 {
			log.Println(http.ListenAndServe(host+":"+strconv.Itoa(*pprofPort), nil))
		}
	}()

	qserver := server.NewKiteQServer(*bindHost, *zkhost, strings.Split(*topics, ","), *mysql)

	qserver.Start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Kill)

	select {
	//kill掉的server
	case <-ch:

	}

	qserver.Shutdown()
	log.Println("KiteQServer IS STOPPED!")
}
