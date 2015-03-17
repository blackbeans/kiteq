#!/bin/bash

go get github.com/golang/protobuf/{proto,protoc-gen-go}
go get github.com/blackbeans/go-uuid
go get github.com/blackbeans/zk


protoc --go_out=. ./protocol/*.proto

go build -a kiteq/stat
go build -a kiteq/protocol
go build -a kiteq/binding
go build -a kiteq/store
go build -a kiteq/store/mysql
go build -a kiteq/store/mmap
go build -a kiteq/pipe
go build -a kiteq/handler
go build -a kiteq/remoting/session
go build -a kiteq/remoting/server
go build -a kiteq/remoting/client
go build -a kiteq/client/chandler
go build -a kiteq/server
go build -a kiteq/client


#########
go install kiteq/stat
go install kiteq/protocol
go install kiteq/binding
go install kiteq/store
go install kiteq/store/mysql
go install kiteq/store/mmap
go install kiteq/pipe
go install kiteq/handler
go install kiteq/remoting/session
go install kiteq/remoting/server
go install kiteq/remoting/client
go install kiteq/client/chandler
go install kiteq/server
go install kiteq/client



##############
echo "------------ compoments  installing is finished!-------------"

PROJ=`pwd | awk -F'/' '{print $(NF)}'`
#VERSION=$1
#go build  -o ./$PROJ-$VERSION $PROJ.go
go build  -a -o ./$PROJ $PROJ.go

go build -a kite_benchmark_producer.go
go build -a kite_benchmark_consumer.go







