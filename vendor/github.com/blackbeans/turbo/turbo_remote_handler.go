package turbo

import (
	log "github.com/blackbeans/log4go"
	"math/rand"
)

//没有链接的分组直接失败
var EMPTY_FUTURE = make(map[string]*Future, 0)

//远程操作的remotinghandler

type RemotingHandler struct {
	BaseForwardHandler
	clientManager *ClientManager
}

func NewRemotingHandler(name string, clientManager *ClientManager) *RemotingHandler {
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
	var futures map[string]*Future
	if len(revent.errFutures) <= 0 && len(revent.GroupIds) <= 0 && len(revent.TargetHost) <= 0 {
		log.Warn("RemotingHandler|%s|Process|NO GROUP OR HOSTS|%s|%s\n", self.GetName(), revent)
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

func (self *RemotingHandler) invokeGroup(event *RemotingEvent) map[string]*Future {

	//特别的失败分组，为了减少chan的创建数
	futures := make(map[string]*Future, 10)
	packet := *event.Packet
	if len(event.TargetHost) > 0 {
		//特定机器
		for _, host := range event.TargetHost {
			rclient := self.clientManager.FindTClient(host)
			if nil != rclient && !rclient.IsClosed() {
				//写到响应的channel中
				f, err := rclient.Write(packet)
				if nil != err {
					futures[host] = NewErrFuture(-1, rclient.RemoteAddr(), err, rclient.ctx)
				} else {
					futures[host] = f
				}

			} else {
				//记为失败的下次需要重新发送
				// log.Debug("RemotingHandler|%s|invokeGroup|NO RemoteClient|%s\n", self.GetName(), host)
				futures[host] = NewErrFuture(-1, "no remoteclient", ERR_NO_HOSTS, nil)
			}
		}
	}

	if len(event.GroupIds) > 0 {
		clients := self.clientManager.FindTClients(event.GroupIds, func(groupId string, rc *TClient) bool {
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
			f, err := c[idx].Write(packet)
			if nil != err {
				futures[gid] = NewErrFuture(-1, c[idx].RemoteAddr(), err, c[idx].ctx)
			} else {
				f.TargetHost = c[idx].RemoteAddr()
				futures[gid] = f
			}
		}
	}

	//增加错误的future
	if nil != event.errFutures {
		for k, v := range event.errFutures {
			// log.InfoLog("kite_handler", "%v----------%v---------------%v", k, v)
			futures[k] = v

		}
	}

	//统计哪些不在线的分组
	for _, g := range event.GroupIds {
		_, ok := futures[g]
		if !ok {
			futures[g] = NewErrFuture(-1, g, ERR_NO_HOSTS, nil)
		}
	}

	return futures
}
