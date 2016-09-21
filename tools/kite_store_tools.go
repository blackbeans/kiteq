package main

import (
	"flag"
	"fmt"
	"github.com/blackbeans/kiteq-common/store/parser"
	"time"
)

func main() {
	db := flag.String("db", "", "-db")
	serverTag := flag.String("serverTag", "", "default")
	flag.Parse()
	store := parser.ParseDB(*db, *serverTag)
	store.Start()
	count := 0
	for i := 0; i < 16; i++ {
		_, entities := store.PageQueryEntity(fmt.Sprintf("%x", i), "default", time.Now().Unix(), 0, 1000)
		count += len(entities)
		//fmt.Printf("%s,%v\n", len(entities), string(entities[0].Body.([]byte)))

	}

	time.Sleep(5 * time.Minute)

	fmt.Printf("----------end|%d\n", count)
}
