package main

import (
	"context"
	"flag"
	"fmt"
	"kiteq/store/parser"
	"time"
)

func main() {
	db := flag.String("db", "", "-db")
	serverTag := flag.String("serverTag", "", "default")
	flag.Parse()
	store := parser.ParseDB(context.TODO(), *db, *serverTag)
	store.Start()
	count := 0
	for i := 0; i < 16; i++ {
		_, entities := store.PageQueryEntity(fmt.Sprintf("%x", i), "default", time.Now().Unix(), 0, 1000)
		count += len(entities)
		//fmt.Printf("%s,%v", len(entities), string(entities[0].Body.([]byte)))

	}

	time.Sleep(5 * time.Minute)

	fmt.Printf("----------end|%d", count)
}
