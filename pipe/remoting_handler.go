package pipe

import (
	"kiteq/remoting/client"
	"kiteq/stat"
	"log"
	"math/rand"
)

//没有链接的分组直接失败
var EMPTY_FUTURE = make(map[string]chan interface{}, 0)
var QUICK_FAILGROUP_FUTURE = make(chan interface{}, 1)

func init() {
	close(QUICK_FAILGROUP_FUTURE)
}

//远程操作的remotinghandler

type RemotingHandler struct {
	BaseForwardHandler
	clientManager *client.ClientManager
	flowControl   *stat.FlowControl
}

func NewRemotingHandler(name string, clientManager *client.ClientManager, flowControl *stat.FlowControl) *RemotingHandler {
	remtingHandler := &RemotingHandler{}
	remtingHandler.BaseForwardHandler = NewBaseForwardHandler(name, remtingHandler)
	remtingHandler.clientManager = clientManager
	remtingHandler.flowControl = flowControl
	return remtingHandler
}

func (self *RemotingHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *RemotingHandler) cast(event IEvent) (val *RemotingEvent, ok bool) {
	val, ok = event.(*RemotingEvent)
	return
}

func (self *RemotingHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	revent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	// log.Printf("RemotingHandler|Process|%s|%t\n", self.GetName(), revent)
	var futures map[string]chan interface{}
	if len(revent.GroupIds) <= 0 && len(revent.TargetHost) <= 0 {
		log.Printf("RemotingHandler|%s|Process|NO GROUP OR HOSTS|%s|%s\n", self.GetName(), revent)
		futures = EMPTY_FUTURE
	} else {
		//发送数据
		futures = self.invokeGroup(revent)
	}
	//写入future的响应
	revent.futures <- futures
	close(revent.futures)

	//创建创建网络写出结果
	fe := NewRemoteFutureEvent(revent, futures)
	ctx.SendForward(fe)
	return nil
}

func (self *RemotingHandler) invokeSingle(event *RemotingEvent) error {
	return nil
}

func (self *RemotingHandler) invokeGroup(event *RemotingEvent) map[string]chan interface{} {

	//特别的失败分组，为了减少chan的创建数
	futures := make(map[string]chan interface{}, 10)
	packet := *event.Packet
	if len(event.TargetHost) > 0 {
		//特定机器
		for _, host := range event.TargetHost {
			rclient := self.clientManager.FindRemoteClient(host)
			if nil != rclient && !rclient.IsClosed() {
				//写到响应的channel中
				futures[host] = rclient.Write(packet)
				self.flowControl.WriteFlow.Incr(1)

			} else {
				//记为失败的下次需要重新发送
				log.Printf("RemotingHandler|%s|invokeGroup|NO RemoteClient|%s\n", self.GetName(), host)
				futures[host] = FAILGROUP_FUTURE
			}
		}
	}

	if len(event.GroupIds) > 0 {
		clients := self.clientManager.FindRemoteClients(event.GroupIds, func(groupId string, rc *client.RemotingClient) bool {
			//过滤条件进行
			if rc.IsClosed() {
				return true
			}
			return false
		})

		//发送组内的随机选择一个
		for gid, c := range clients {
			if len(c) <= 0 {
				continue
			}
			idx := rand.Intn(len(c))
			//克隆一份
			futures[gid] = c[idx].Write(packet)
			self.flowControl.WriteFlow.Incr(1)
		}
	}

	//统计哪些不在线的分组
	for _, g := range event.GroupIds {
		_, ok := futures[g]
		if !ok {
			futures[g] = FAILGROUP_FUTURE
		}
	}

	return futures
}
