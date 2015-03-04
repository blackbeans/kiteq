package core

import (
	"errors"
	"kiteq/protocol"
	"kiteq/remoting/client"
	"log"
	"time"
)

//握手包
func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {

	for i := 0; i < 3; i++ {
		packet := protocol.MarshalConnMeta(ga.GroupId, ga.SecretKey)
		rpacket := protocol.NewPacket(protocol.CMD_CONN_META, packet)
		rpacket.BlockingWrite()
		resp, err := remoteClient.WriteAndGet(*rpacket, 5*time.Second)
		if nil != err {
			//两秒后重试
			time.Sleep(2 * time.Second)
			log.Printf("KiteQServer|handShake|FAIL|%s|%s\n", ga.GroupId, err)
		} else {
			authAck, ok := resp.(*protocol.ConnAuthAck)
			if !ok {
				return false, errors.New("Unmatches Handshake Ack Type! ")
			} else {
				if authAck.GetStatus() {
					log.Printf("KiteQServer|handShake|SUCC|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return true, nil
				} else {
					log.Printf("KiteQServer|handShake|FAIL|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return false, errors.New("Auth FAIL![" + authAck.GetFeedback() + "]")
				}
			}
		}
	}

	return false, errors.New("handshake fail!")
}
