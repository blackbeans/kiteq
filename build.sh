#!/bin/bash

go get github.com/golang/groupcache/lru
go get github.com/golang/protobuf/{proto,protoc-gen-go}
go get github.com/blackbeans/go-uuid
go get github.com/go-sql-driver/mysql
go get github.com/blackbeans/log4go
go get github.com/blackbeans/go-zookeeper/zk
go get github.com/blackbeans/turbo
go get github.com/naoina/toml
go get github.com/blackbeans/kiteq-common/stat
go get github.com/blackbeans/kiteq-common/protocol
go get github.com/blackbeans/kiteq-common/registry
go get github.com/blackbeans/kiteq-common/registry/bind
go get github.com/blackbeans/kiteq-common/exchange
go get github.com/blackbeans/kiteq-common/store
go get github.com/blackbeans/kiteq-common/store/mysql
go get github.com/blackbeans/kiteq-common/store/file
go get github.com/blackbeans/kiteq-common/store/memory


#protoc --go_out=. ./protocol/*.proto
go build  kiteq/handler

go build  kiteq/server


#########
go install kiteq/handler
go install kiteq/server



##############
echo "------------ compoments  installing is finished!-------------"

PROJ=`pwd | awk -F'/' '{print $(NF)}'`
#VERSION=$1
#go build  -o ./$PROJ-$VERSION $PROJ.go
go build  -a -o ./$PROJ $PROJ.go

tar -zcvf kiteq.tar.gz $PROJ log/log.xml conf/*.toml






