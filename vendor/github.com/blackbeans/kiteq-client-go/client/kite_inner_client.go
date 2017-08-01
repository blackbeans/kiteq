package client

import (
	"errors"
	"fmt"
	"github.com/blackbeans/kiteq-common/protocol"
	c "github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/packet"
	// 	log "github.com/blackbeans/log4go"
	"time"
)

type kiteClient struct {
	remotec *c.RemotingClient
}

func newKitClient(remoteClient *c.RemotingClient) *kiteClient {

	client := &kiteClient{
		remotec: remoteClient}

	return client
}

//发送事务的确认,无需等待服务器反馈
func (self *kiteClient) sendTxAck(message *protocol.QMessage,
	txstatus protocol.TxStatus, feedback string) error {
	//写入时间
	if message.GetHeader().GetCreateTime() <= 0 {
		message.GetHeader().CreateTime = protocol.MarshalInt64(time.Now().Unix())
	}
	txpacket := protocol.MarshalTxACKPacket(message.GetHeader(), txstatus, feedback)
	return self.innerSendMessage(protocol.CMD_TX_ACK, txpacket, 0)
}

func (self *kiteClient) sendMessage(message *protocol.QMessage) error {
	//写入时间
	if message.GetHeader().GetCreateTime() <= 0 {
		message.GetHeader().CreateTime = protocol.MarshalInt64(time.Now().Unix())
	}
	data, err := protocol.MarshalPbMessage(message.GetPbMessage())
	if nil != err {
		return err
	}
	timeout := 3 * time.Second
	return self.innerSendMessage(message.GetMsgType(), data, timeout)
}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

func (self *kiteClient) innerSendMessage(cmdType uint8, p []byte, timeout time.Duration) error {

	msgpacket := packet.NewPacket(cmdType, p)

	//如果是需要等待结果的则等待
	if timeout <= 0 {
		_, err := self.remotec.Write(*msgpacket)
		return err
	} else {
		resp, err := self.remotec.WriteAndGet(*msgpacket, timeout)
		if nil != err {
			return err
		} else {
			storeAck, ok := resp.(*protocol.MessageStoreAck)
			if !ok || !storeAck.GetStatus() {
				return errors.New(fmt.Sprintf("kiteClient|SendMessage|FAIL|%s\n", resp))
			} else {
				//log.DebugLog("kite_client","kiteClient|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
				return nil
			}
		}
	}
}
