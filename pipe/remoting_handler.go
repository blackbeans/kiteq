package pipe

import (
	"kiteq/remoting/client"
	"kiteq/stat"
	"log"
	"math/rand"
)

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

var EMPTY_FUTURE = make(map[string]chan interface{}, 0)

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
	ctx.SendBackward(fe)
	return nil
}

func (self *RemotingHandler) invokeSingle(event *RemotingEvent) error {
	return nil
}

func (self *RemotingHandler) invokeGroup(event *RemotingEvent) map[string]chan interface{} {

	//特别的失败分组，为了减少chan的创建数
	failGroup := make(chan interface{}, 1)
	futures := make(map[string]chan interface{}, 10)
	if len(event.TargetHost) > 0 {
		//特定机器
		for _, host := range event.TargetHost {
			rclient := self.clientManager.FindRemoteClient(host)
			if nil != rclient {
				//写到响应的channel中
				futures[host] = rclient.Write(event.Packet)
				self.flowControl.WriteFlow.Incr(1)

			} else {
				//记为失败的下次需要重新发送
				log.Printf("RemotingHandler|%s|invokeGroup|NO RemoteClient|%s|%s\n", self.GetName(), host, event.Packet)
			}
		}
	}

	if len(event.GroupIds) > 0 {
		clients := self.clientManager.FindRemoteClients(event.GroupIds, func(groupId string) bool {
			//过滤条件进行
			return false
		})

		//发送组内的随机选择一个
		for gid, c := range clients {
			if len(c) <= 0 {
				continue
			}
			idx := rand.Intn(len(c))
			futures[gid] = c[idx].Write(event.Packet)
			self.flowControl.WriteFlow.Incr(1)
		}
	}

	//统计哪些不在线的host
	for _, h := range event.TargetHost {
		_, ok := futures[h]
		if !ok {
			futures[h] = failGroup
		}
	}

	//统计哪些不在线的分组
	for _, g := range event.GroupIds {
		_, ok := futures[g]
		if !ok {
			futures[g] = failGroup
		}
	}

	return futures
}
