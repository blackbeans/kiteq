package client

import (
	"errors"
	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	c "github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/packet"
	"time"
)

//握手包
func handshake(ga *c.GroupAuth, remoteClient *c.RemotingClient) (bool, error) {

	for i := 0; i < 3; i++ {
		p := protocol.MarshalConnMeta(ga.GroupId, ga.SecretKey, int32(ga.WarmingupSec))
		rpacket := packet.NewPacket(protocol.CMD_CONN_META, p)
		resp, err := remoteClient.WriteAndGet(*rpacket, 5*time.Second)
		if nil != err {
			//两秒后重试
			time.Sleep(2 * time.Second)
			log.WarnLog("kite_client", "kiteClient|handShake|FAIL|%s|%s\n", ga.GroupId, err)
		} else {
			authAck, ok := resp.(*protocol.ConnAuthAck)
			if !ok {
				return false, errors.New("Unmatches Handshake Ack Type! ")
			} else {
				if authAck.GetStatus() {
					log.InfoLog("kite_client", "kiteClient|handShake|SUCC|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return true, nil
				} else {
					log.WarnLog("kite_client", "kiteClient|handShake|FAIL|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return false, errors.New("Auth FAIL![" + authAck.GetFeedback() + "]")
				}
			}
		}
	}

	return false, errors.New("handshake fail! [" + remoteClient.RemoteAddr() + "]")
}
