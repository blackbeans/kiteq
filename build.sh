#!/bin/bash

#protoc --go_out=. ./protocol/*.proto

##############
echo "------------ compoments  installing is finished!-------------"

PROJ=`pwd | awk -F'/' '{print $(NF)}'`
#VERSION=$1
go build  -o ./$PROJ $PROJ.go


#CGO_ENABLED=0 GOOS=linux GOARCH=386 go build  -a -o ./$PROJ $PROJ.go
#GOOS=darwin GOARCH=386  go build  -a -o ./$PROJ $PROJ.go

#tar -zcvf kiteq-1.0.2-linux-386.tar.gz $PROJ log/log.xml conf/*.toml
#tar -zcvf kiteq-1.0.2-darwin-386.tar.gz $PROJ log/log.xml conf/*.toml
tar -zcvf kiteq.tar.gz $PROJ conf/*.toml
