#!/bin/bash

go get github.com/golang/groupcache/lru
go get github.com/golang/protobuf/{proto,protoc-gen-go}
go get github.com/blackbeans/go-uuid
go get github.com/go-sql-driver/mysql
go get github.com/blackbeans/log4go
go get github.com/blackbeans/go-zookeeper/zk
go get github.com/blackbeans/turbo
go get github.com/naoina/toml


#protoc --go_out=. ./protocol/*.proto

go build  kiteq/stat
go build  kiteq/protocol
go build  kiteq/binding
go build  kiteq/store
go build  kiteq/store/mysql
go build  kiteq/store/file
go build  kiteq/store/memory
go build  kiteq/handler
go build  kiteq/client/chandler
go build  kiteq/server
go build  kiteq/client


#########
go install kiteq/stat
go install kiteq/protocol
go install kiteq/binding
go install kiteq/store
go install kiteq/store/mysql
go install kiteq/store/memory
go install kiteq/store/file
go install kiteq/handler
go install kiteq/client/chandler
go install kiteq/server
go install kiteq/client



##############
echo "------------ compoments  installing is finished!-------------"

PROJ=`pwd | awk -F'/' '{print $(NF)}'`
#VERSION=$1
#go build  -o ./$PROJ-$VERSION $PROJ.go
go build  -a -o ./$PROJ $PROJ.go

tar -zcvf kiteq.tar.gz $PROJ log/log.xml conf/*.toml






