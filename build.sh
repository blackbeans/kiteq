#!/bin/bash


go get  github.com/golang/protobuf/{proto,protoc-gen-go}
go get  github.com/blackbeans/go-uuid
go get  github.com/go-sql-driver/mysql
go get -u github.com/blackbeans/log4go
go get -u github.com/blackbeans/go-zookeeper/zk
go get -u  github.com/blackbeans/turbo


protoc --go_out=. ./protocol/*.proto

go build -a kiteq/stat
go build -a kiteq/protocol
go build -a kiteq/binding
go build -a kiteq/store
go build -a kiteq/store/mysql
go build -a kiteq/store/file
go build -a kiteq/store/memory
go build -a kiteq/handler
go build -a kiteq/client/chandler
go build -a kiteq/server
go build -a kiteq/client


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

tar -zcvf kiteq.tar.gz $PROJ log/log.xml

go build -a kite_benchmark_producer.go
go build -a kite_benchmark_consumer.go







