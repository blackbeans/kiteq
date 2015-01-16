#!/bin/bash


go build go-kite/protocol
go build go-kite/store
go build go-kite/handler
go build go-kite/remoting/server
go build go-kite/remoting/session
go build go-kite/client


#########
go install go-kite/protocol
go install go-kite/store
go install go-kite/handler
go install go-kite/remoting/server
go install go-kite/remoting/session
go install go-kite/client







