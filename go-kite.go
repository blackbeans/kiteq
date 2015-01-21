package main

import (
	"flag"
	"go-kite/handler"
	"go-kite/remoting/server"
	"go-kite/store"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {

	bindHost := flag.String("bind", ":13800", "-bind=localhost:13800")
	flag.Parse()
	//初始化pipeline
	pipeline := handler.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet"))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access"))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", &store.MockKiteStore{}))
	pipeline.RegisteHandler("remoting", handler.NewRemotingHandler("remoting"))

	remotingServer := server.NewRemotionServer(*bindHost, 3*time.Second, pipeline)
	stopCh := make(chan error, 1)
	go func() {
		err := remotingServer.ListenAndServer()
		stopCh <- err
	}()

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
