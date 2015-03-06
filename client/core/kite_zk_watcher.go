package core

import (
	"kiteq/binding"
	"kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"log"
	"sort"
	"strings"
)

func (self *KiteClientManager) NodeChange(path string, eventType binding.ZkEvent, children []string) {
	// @todo关闭或者新增相应的pub/sub connections
	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, binding.KITEQ_SERVER) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.Printf("KiteClientManager|ChildWatcher|INVALID SERVER PATH |%s|%t\n", path, children)
			return
		}
		//获取topic
		topic := split[3]
		//不是当前服务可以处理的topic则直接丢地啊哦
		if sort.SearchStrings(self.topics, topic) == len(self.topics) {
			log.Printf("BindExchanger|ChildWatcher|REFUSE SERVER PATH |%s|%t\n", path, children)
			return
		}
		self.onQServerChanged(topic, children)
	}
}

//当触发QServer地址发生变更
func (self *KiteClientManager) onQServerChanged(topic string, hosts []string) {

	//重建一下topic下的kiteclient
	clients := make([]*kiteClient, 0, 10)
	for _, host := range hosts {
		//如果能查到remoteClient 则直接复用
		remoteClient := self.clientManager.FindRemoteClient(host)
		if nil == remoteClient {
			//这里就新建一个remote客户端连接
			conn, err := dial(host)
			if nil != err {
				log.Printf("KiteClientManager|onQServerChanged|Create REMOTE CLIENT|FAIL|%s|%s\n", err, host)
				continue
			}
			remoteClient = rclient.NewRemotingClient(conn,
				func(rc *rclient.RemotingClient, packet *protocol.Packet) {
					event := pipe.NewPacketEvent(rc, packet)
					err := self.pipeline.FireWork(event)
					if nil != err {
						log.Printf("KiteClientManager|onPacketRecieve|FAIL|%s|%t\n", err, packet)
					}
				}, self.rc)
			remoteClient.Start()
			auth, err := handshake(self.ga, remoteClient)
			if !auth || nil != err {
				remoteClient.Shutdown()
				log.Printf("KiteClientManager|onQServerChanged|HANDSHAKE|FAIL|%s|%s\n", err, auth)
				continue
			}
			self.clientManager.Auth(self.ga, remoteClient)
		}

		//创建kiteClient
		kiteClient := newKitClient(remoteClient)
		clients = append(clients, kiteClient)
		log.Printf("KiteClientManager|onQServerChanged|newKitClient|SUCC|%s\n", host)
	}

	self.lock.Lock()
	defer self.lock.Unlock()
	//替换掉线的server
	old, ok := self.kiteClients[topic]
	self.kiteClients[topic] = clients
	if ok {
		del := make([]string, 0, 2)
	outter:
		for _, o := range old {
			for _, c := range clients {
				if c.remoteClient.RemoteAddr() == o.remoteClient.RemoteAddr() {
					continue outter
				}
			}
			del = append(del, o.remoteClient.RemoteAddr())
		}

		if len(del) > 0 {
			self.clientManager.DeleteClients(del...)
		}
	}
}

func (self *KiteClientManager) DataChange(path string, binds []*binding.Binding) {
	//IGNORE
}
