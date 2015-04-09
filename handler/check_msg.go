package handler

import (
	packet "github.com/blackbeans/turbo/packet"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/protocol"
	"regexp"
	"sort"
	"time"
)

const (
	MAX_EXPIRED_TIME  = 7 * 24 * 3600 * time.Second
	MAX_DELIVER_LIMIT = 100
)

var rc *regexp.Regexp

func init() {
	rc = regexp.MustCompile("[0-9a-fA-F]{32}")
}

//----------------持久化的handler
type CheckMessageHandler struct {
	BaseForwardHandler
	topics []string
}

//------创建persitehandler
func NewCheckMessageHandler(name string, topics []string) *CheckMessageHandler {
	phandler := &CheckMessageHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	sort.Strings(topics)
	phandler.topics = topics
	return phandler
}

func (self *CheckMessageHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *CheckMessageHandler) cast(event IEvent) (val *persistentEvent, ok bool) {
	val, ok = event.(*persistentEvent)
	return
}

func (self *CheckMessageHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	if nil != pevent.entity {

		//先判断是否是可以处理的topic的消息
		idx := sort.SearchStrings(self.topics, pevent.entity.Header.GetTopic())
		if idx == len(self.topics) {
			//不存在该消息的处理则直接返回存储失败
			remoteEvent := NewRemotingEvent(storeAck(pevent.opaque,
				pevent.entity.Header.GetMessageId(), false, "UnSupport Topic Message!"),
				[]string{pevent.remoteClient.RemoteAddr()})
			ctx.SendForward(remoteEvent)
		} else if !isUUID(pevent.entity.Header.GetMessageId()) {
			//不存在该消息的处理则直接返回存储失败
			remoteEvent := NewRemotingEvent(storeAck(pevent.opaque,
				pevent.entity.Header.GetMessageId(), false, "Invalid MessageId For UUID!"),
				[]string{pevent.remoteClient.RemoteAddr()})
			ctx.SendForward(remoteEvent)
		} else {
			//对头部的数据进行校验设置
			h := pevent.entity.Header
			if h.GetDeliverLimit() <= 0 || h.GetDeliverLimit() > MAX_DELIVER_LIMIT {
				h.DeliverLimit = protocol.MarshalInt32(MAX_DELIVER_LIMIT)
			}
			if h.GetExpiredTime() <= 0 || h.GetExpiredTime() > time.Now().Add(MAX_EXPIRED_TIME).Unix() {
				h.ExpiredTime = protocol.MarshalInt64(int64(MAX_EXPIRED_TIME))
			} else if h.GetExpiredTime() > 0 && h.GetExpiredTime() <= time.Now().Unix() {
				//不存在该消息的处理则直接返回存储失败
				remoteEvent := NewRemotingEvent(storeAck(pevent.opaque,
					pevent.entity.Header.GetMessageId(), false, "Expired Message!"),
					[]string{pevent.remoteClient.RemoteAddr()})
				ctx.SendForward(remoteEvent)
				return nil
			}
			//向后发送
			ctx.SendForward(pevent)
		}
	}

	return nil
}

func isUUID(id string) bool {

	if len(id) > 32 || !rc.MatchString(id) {
		return false
	}
	return true
}

func storeAck(opaque int32, messageid string, succ bool, feedback string) *packet.Packet {

	storeAck := protocol.MarshalMessageStoreAck(messageid, succ, feedback)
	//响应包
	return packet.NewRespPacket(opaque, protocol.CMD_MESSAGE_STORE_ACK, storeAck)
}
