package core

import (
	"errors"
	"fmt"
	"kiteq/protocol"
	rcient "kiteq/remoting/client"
	"log"
	"time"
)

type kiteClient struct {
	groupId      string
	remoteClient *rcient.RemotingClient
}

func newKitClient(groupId string, remoteClient *rcient.RemotingClient) *kiteClient {

	client := &kiteClient{
		groupId:      groupId,
		remoteClient: remoteClient}

	return client
}

func (self *kiteClient) sendStringMessage(message *protocol.StringMessage) error {
	data, err := protocol.MarshalPbMessage(message)
	if nil != err {
		return err
	}
	return self.innerSendMessage(protocol.CMD_STRING_MESSAGE, data)
}

func (self *kiteClient) sendBytesMessage(message *protocol.BytesMessage) error {
	data, err := protocol.MarshalPbMessage(message)
	if nil != err {
		return err
	}

	return self.innerSendMessage(protocol.CMD_BYTES_MESSAGE, data)
}

func (self *kiteClient) innerSendMessage(cmdType uint8, packet []byte) error {
	msgpacket := protocol.NewPacket(cmdType, packet)

	resp, err := self.remoteClient.WriteAndGet(msgpacket, 200*time.Millisecond)
	if nil != err {
		return err
	} else {
		storeAck, ok := resp.(*protocol.MessageStoreAck)
		if !ok || !storeAck.GetStatus() {
			return errors.New(fmt.Sprintf("kiteClient|SendMessage|FAIL|%s\n", resp))
		} else {
			log.Printf("kiteClient|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
			return nil
		}
	}
}

func (self *kiteClient) closed() bool {
	return self.remoteClient.IsClosed()
}

func (self *kiteClient) close() {
	self.remoteClient.Shutdown()
}
