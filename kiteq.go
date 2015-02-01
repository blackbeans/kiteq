package main

import (
	"flag"
	"kiteq/binding"
	"kiteq/handler"
	"kiteq/pipe"
	"kiteq/remoting/client"
	"kiteq/remoting/server"
	"kiteq/store"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"time"
)

func main() {

	bindHost := flag.String("bind", ":13800", "-bind=localhost:13800")
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

	var kitedb store.IKiteStore
	if *mysql == "" {
		kitedb = &store.MockKiteStore{}
	} else {
		kitedb = store.NewKiteMysql(*mysql)
	}
	// 临时在这里注册一下
	// zk := binding.NewZKManager("")
	// zk.PublishQServer(*bindHost, []string{"trade"})
	// 临时在这里创建的SessionManager

	clientManager := client.NewClientManager()
	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger("localhost:2181")

	//初始化pipeline
	pipeline := pipe.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet"))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", exchanger))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver", kitedb))
	pipeline.RegisteHandler("remoting", handler.NewRemotingHandler("remoting", clientManager))

	remotingServer := server.NewRemotionServer(*bindHost, 3*time.Second,
		func(rclient *client.RemotingClient, packet []byte) {
			event := pipe.NewPacketEvent(rclient, packet)
			err := pipeline.FireWork(event)
			if nil != err {
				log.Printf("RemotingServer|onPacketRecieve|FAIL|%s|%t\n", err, packet)
			}
		})

	stopCh := make(chan error, 1)
	go func() {
		err := remotingServer.ListenAndServer()
		stopCh <- err
	}()

	//推送可发送的topic列表
	exchanger.PushQServer(*bindHost, []string{"trade"})

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Kill)

	select {
	//kill掉的server
	case <-ch:
	case <-stopCh:
	}

	remotingServer.Shutdown()
	log.Println("RemotingServer IS STOPPED!")
}
