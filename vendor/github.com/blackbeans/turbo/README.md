##### turbo
turbo is a lightweight  network framework for golang
 
##### Install

go get github.com/blackbeans/turbo


#### benchmark

main/turbo_server_demo.go

main/turbo_client_demo.go

env: 

	2.5 GHz Intel Core i7  16GRAM  macbook pro

	1 connection 1 groutine  70000 tps

#### 协议定义

	总包长(不包含本4B) 请求的seqId 类型  协议的版本号  扩展预留字段  body的长度	Body
 	---------------------------------------------------------------------------
 	|Length(4B)|Opaque(4B)|CmdType(1B)|Version(2B)|Extension(8B)|BodyLen(4B)|Body|
 	---------------------------------------------------------------------------


##### quickstart
	
	main/turbo_server_demo.go

	main/turbo_client_demo.go

