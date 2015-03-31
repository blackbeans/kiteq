package core

import (
	"errors"
	"fmt"
	"kiteq/protocol"
	"kiteq/remoting/client"
	// 	log "github.com/blackbeans/log4go"
	"time"
)

type kiteClient struct {
	remoteClient *client.RemotingClient
}

func newKitClient(remoteClient *client.RemotingClient) *kiteClient {

	client := &kiteClient{
		remoteClient: remoteClient}

	return client
}

//发送事务的确认,无需等待服务器反馈
func (self *kiteClient) sendTxAck(message *protocol.QMessage,
	txstatus protocol.TxStatus, feedback string) error {
	txpacket := protocol.MarshalTxACKPacket(message.GetHeader(), txstatus, feedback)
	return self.innerSendMessage(protocol.CMD_TX_ACK, txpacket, 0)
}

func (self *kiteClient) sendMessage(message *protocol.QMessage) error {

	data, err := protocol.MarshalPbMessage(message.GetPbMessage())
	if nil != err {
		return err
	}
	timeout := 3 * time.Second
	return self.innerSendMessage(message.GetMsgType(), data, timeout)
}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

func (self *kiteClient) innerSendMessage(cmdType uint8, packet []byte, timeout time.Duration) error {

	msgpacket := protocol.NewPacket(cmdType, packet)

	//如果是需要等待结果的则等待
	if timeout <= 0 {
		_, err := self.remoteClient.Write(*msgpacket)
		return err
	} else {
		resp, err := self.remoteClient.WriteAndGet(*msgpacket, timeout)
		if nil != err {
			return err
		} else {
			storeAck, ok := resp.(*protocol.MessageStoreAck)
			if !ok || !storeAck.GetStatus() {
				return errors.New(fmt.Sprintf("kiteClient|SendMessage|FAIL|%s\n", resp))
			} else {
				// log.Debug("kiteClient|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
				return nil
			}
		}
	}
}
