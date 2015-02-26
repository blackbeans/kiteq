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

func (self *kiteClient) sendMessage(message interface{}) error {

	bm, bok := message.(*protocol.BytesMessage)
	if bok {
		data, err := protocol.MarshalPbMessage(bm)
		if nil != err {
			return err
		}
		return self.innerSendMessage(protocol.CMD_BYTES_MESSAGE, data)
	} else {
		sm, mok := message.(*protocol.StringMessage)
		if mok {
			data, err := protocol.MarshalPbMessage(sm)
			if nil != err {
				return err
			}
			return self.innerSendMessage(protocol.CMD_STRING_MESSAGE, data)
		} else {
			return errors.New("INALID MESSAGE !")
		}
	}
}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

func (self *kiteClient) innerSendMessage(cmdType uint8, packet []byte) error {

	msgpacket := protocol.NewPacket(cmdType, packet)
	remoteEvent := pipe.NewRemotingEvent(msgpacket, []string{self.hostport})
	err := self.pipeline.FireWork(remoteEvent)
	if nil != err {
		return err
	}

	futures := remoteEvent.Wait()
	fc, ok := futures[self.hostport]
	if !ok {
		return errors.New("ILLEGAL STATUS !")
	}
	var resp interface{}
	//
	select {
	case <-time.After(200 * time.Millisecond):
		//删除掉当前holder
		return TIMEOUT_ERROR
	case resp = <-fc:

		storeAck, ok := resp.(*protocol.MessageStoreAck)
		if !ok || !storeAck.GetStatus() {
			return errors.New(fmt.Sprintf("kiteClient|SendMessage|FAIL|%s\n", resp))
		} else {
			// log.Printf("kiteClient|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
			return nil
		}
	}
}
