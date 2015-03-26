package remoting

import (
	"kiteq/protocol"
	. "kiteq/remoting/client"
	"kiteq/remoting/server"
	"kiteq/stat"
	"log"
	"net"
	"testing"
	"time"
)

var clientManager *ClientManager
var remoteServer *server.RemotingServer
var flow = stat.NewFlowStat("server")
var clientf = stat.NewFlowStat("client")

func clientPacketDispatcher(rclient *RemotingClient, resp *protocol.Packet) {
	clientf.ReadFlow.Incr(1)
	clientf.DispatcherFlow.Incr(1)
	rclient.Attach(resp.Opaque, resp.Data)
}

func packetDispatcher(rclient *RemotingClient, p *protocol.Packet) {
	flow.ReadFlow.Incr(1)
	flow.DispatcherFlow.Incr(1)

	resp := protocol.NewRespPacket(p.Opaque, p.CmdType, p.Data)
	//直接回写回去
	rclient.Write(*resp)
	flow.WriteFlow.Incr(1)
}

func handshake(ga *GroupAuth, remoteClient *RemotingClient) (bool, error) {
	return true, nil
}

func init() {

	rc := protocol.NewRemotingConfig(
		"KiteQ-localhost:28888",
		1000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)
	remoteServer = server.NewRemotionServer("localhost:28888", rc, packetDispatcher)
	remoteServer.ListenAndServer()

	conn, _ := dial("localhost:28888")

	// //重连管理器
	reconnManager := NewReconnectManager(false, -1, -1, handshake)

	clientManager = NewClientManager(reconnManager)

	rcc := protocol.NewRemotingConfig(
		"KiteQ-localhost:28888",
		1000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	remoteClient := NewRemotingClient(conn, clientPacketDispatcher, rcc)
	remoteClient.Start()

	auth := &GroupAuth{}
	auth.GroupId = "a"
	auth.SecretKey = "123"
	clientManager.Auth(auth, remoteClient)
	clientf.Start()

}

func BenchmarkRemoteClient(t *testing.B) {
	t.SetParallelism(4)

	t.RunParallel(func(pb *testing.PB) {
		p := protocol.NewPacket(1, []byte("echo"))
		for pb.Next() {
			for i := 0; i < t.N; i++ {
				tmp := clientManager.FindRemoteClients([]string{"a"}, func(groupid string, c *RemotingClient) bool {
					return false
				})

				_, err := tmp["a"][0].WriteAndGet(*p, 500*time.Millisecond)
				clientf.WriteFlow.Incr(1)
				if nil != err {
					log.Printf("WAIT RESPONSE FAIL|%s\n", err)
				} else {
					// log.Printf("WAIT RESPONSE SUCC|%s\n", string(resp.([]byte)))
				}
			}
		}

	})
}

//创建物理连接
func dial(hostport string) (*net.TCPConn, error) {
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
}
