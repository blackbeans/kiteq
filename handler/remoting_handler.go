package handler

import (
	. "kiteq/pipe"
	"kiteq/remoting/client"
	"log"
	"math/rand"
)

//远程操作的remotinghandler

type RemotingHandler struct {
	BaseForwardHandler
	clientManager *client.ClientManager
}

func NewRemotingHandler(name string, clientManager *client.ClientManager) *RemotingHandler {
	remtingHandler := &RemotingHandler{}
	remtingHandler.BaseForwardHandler = NewBaseForwardHandler(name, remtingHandler)
	remtingHandler.clientManager = clientManager
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

	//发送数据
	self.invokeGroup(revent)
	//创建投递结果事件

	return nil
}

func (self *RemotingHandler) invokeSingle(event *RemotingEvent) error {
	return nil
}

func (self *RemotingHandler) invokeGroup(event *RemotingEvent) map[string]chan interface{} {

	futures := make(map[string]chan interface{}, 10)
	if len(event.TargetHost) > 0 {
		//特定机器
		for _, host := range event.TargetHost {
			rclient := self.clientManager.FindRemoteClient(host)
			if nil != rclient {
				//写到响应的channel中
				futures[host] = rclient.Write(event.Packet)
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
			idx := rand.Intn(len(c))
			futures[gid] = c[idx].Write(event.Packet)
		}
	}

	return futures
}

//都低结果
type deliverResult struct {
	groupId string
	err     error
}
