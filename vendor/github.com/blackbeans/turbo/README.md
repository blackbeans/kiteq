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

server：

```
import (
	"github.com/blackbeans/turbo"
	"github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/packet"
	"github.com/blackbeans/turbo/server"
	"log"
	"time"
)

func packetDispatcher(rclient *client.RemotingClient, p *packet.Packet) {

	resp := packet.NewRespPacket(p.Opaque, p.CmdType, p.Data)
	//直接回写回去
	rclient.Write(*resp)
	log.Printf("packetDispatcher|WriteResponse|%s\n", string(resp.Data))
}

func main() {
	rc := turbo.NewRemotingConfig(
		"turbo-server:localhost:28888",
		1000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	remoteServer := server.NewRemotionServer("localhost:28888", rc, packetDispatcher)
	remoteServer.ListenAndServer()
}

```

client:
```
import (
	"github.com/blackbeans/turbo"
	"github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/packet"
	"log"
	"net"
	"time"
)

func clientPacketDispatcher(rclient *client.RemotingClient, resp *packet.Packet) {
	rclient.Attach(resp.Opaque, resp.Data)
	log.Printf("clientPacketDispatcher|%s\n", string(resp.Data))
}

func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {
	return true, nil
}

func main() {
	// 重连管理器
	reconnManager := client.NewReconnectManager(false, -1, -1, handshake)

	clientManager := client.NewClientManager(reconnManager)

	rcc := turbo.NewRemotingConfig(
		"turbo-client:localhost:28888",
		1000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	//创建物理连接
	conn, _ := func(hostport string) (*net.TCPConn, error) {
		//连接
		remoteAddr, err_r := net.ResolveTCPAddr("tcp4", hostport)
		if nil != err_r {
			log.Printf("KiteClientManager|RECONNECT|RESOLVE ADDR |FAIL|remote:%s\n", err_r)
			return nil, err_r
		}
		conn, err := net.DialTCP("tcp4", nil, remoteAddr)
		if nil != err {
			log.Printf("KiteClientManager|RECONNECT|%s|FAIL|%s\n", hostport, err)
			return nil, err
		}

		return conn, nil
	}("localhost:28888")

	remoteClient := client.NewRemotingClient(conn, clientPacketDispatcher, rcc)
	remoteClient.Start()

	auth := &client.GroupAuth{}
	auth.GroupId = "a"
	auth.SecretKey = "123"
	clientManager.Auth(auth, remoteClient)

	//echo command
	p := packet.NewPacket(1, []byte("echo"))

	//find a client
	tmp := clientManager.FindRemoteClients([]string{"a"}, func(groupid string, c *client.RemotingClient) bool {
		return false
	})

	//write command and wait for response
	_, err := tmp["a"][0].WriteAndGet(*p, 500*time.Millisecond)
	if nil != err {
		log.Printf("WAIT RESPONSE FAIL|%s\n", err)
	} else {
		// log.Printf("WAIT RESPONSE SUCC|%s\n", string(resp.([]byte)))
	}

}

```

