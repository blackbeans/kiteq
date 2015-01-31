#!/bin/bash

go get github.com/golang/protobuf/{proto,protoc-gen-go}
go get github.com/go-sql-driver/mysql
go get github.com/blackbeans/zk
go get github.com/BurntSushi/toml

protoc --go_out=. ./protocol/*.proto

go build kiteq/stat
go build kiteq/binding
go build kiteq/protocol
go build kiteq/store
go build kiteq/handler
go build kiteq/remoting/session
go build kiteq/remoting/server
go build kiteq/remoting/client
go build kiteq/client


#########
go install kiteq/stat
go install kiteq/binding
go install kiteq/protocol
go install kiteq/store
go install kiteq/handler
go install kiteq/remoting/session
go install kiteq/remoting/server
go install kiteq/remoting/client
go install kiteq/client


##############
echo "------------ compoments  installing is finished!-------------"

PROJ=`pwd | awk -F'/' '{print $(NF)}'`
#VERSION=$1
#go build  -o ./$PROJ-$VERSION $PROJ.go
go build  -o ./$PROJ $PROJ.go

go build kite_benchmark.go






