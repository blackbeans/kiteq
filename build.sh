#!/bin/bash

go get github.com/golang/protobuf/{proto,protoc-gen-go}
go get github.com/blackbeans/go-uuid
go get github.com/go-sql-driver/mysql
go get github.com/blackbeans/zk
go get -u github.com/sutoo/gorp

protoc --go_out=. ./protocol/*.proto

go build kiteq/stat
go build kiteq/protocol
go build kiteq/binding
go build kiteq/store
go build kiteq/pipe
go build kiteq/handler
go build kiteq/remoting/session
go build kiteq/remoting/server
go build kiteq/remoting/client
go build kiteq/client/chandler
go build kiteq/server
go build kiteq/client


#########
go install kiteq/stat
go install kiteq/protocol
go install kiteq/binding
go install kiteq/store
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
go build  -o ./$PROJ $PROJ.go

go build kite_benchmark_producer.go
go build kite_benchmark_consumer.go







