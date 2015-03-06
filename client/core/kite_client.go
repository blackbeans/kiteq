package core

import (
	"errors"
	"fmt"
	"kiteq/pipe"
	"kiteq/protocol"
	// "log"
	"time"
)

type kiteClient struct {
	hostport string
	pipeline *pipe.DefaultPipeline
}

func newKitClient(hostport string, pipeline *pipe.DefaultPipeline) *kiteClient {

	client := &kiteClient{
		hostport: hostport,
		pipeline: pipeline}

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
	//如果是fly消息不需要客户端等等
	if message.GetHeader().GetFly() {
		timeout = 0 * time.Second
	}
	return self.innerSendMessage(message.GetMsgType(), data, timeout)
}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

func (self *kiteClient) innerSendMessage(cmdType uint8, packet []byte, timeout time.Duration) error {

	msgpacket := protocol.NewPacket(cmdType, packet)
	remoteEvent := pipe.NewRemotingEvent(msgpacket, []string{self.hostport})
	err := self.pipeline.FireWork(remoteEvent)
	//如果是需要等待结果的则等待
	if nil != err {
		return err
	}

	if timeout <= 0 {
		return nil
	}

	futures := remoteEvent.Wait()
	fc, ok := futures[self.hostport]
	if !ok {
		return errors.New("ILLEGAL STATUS !")
	}

	var resp interface{}
	select {
	case resp = <-fc:
		storeAck, ok := resp.(*protocol.MessageStoreAck)
		if !ok || !storeAck.GetStatus() {
			return errors.New(fmt.Sprintf("kiteClient|SendMessage|FAIL|%s\n", resp))
		} else {
			// log.Printf("kiteClient|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
			return nil
		}
	case <-time.After(timeout):
		//删除掉当前holder
		return TIMEOUT_ERROR
	}

}
