package client

import (
	"strings"

	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/kiteq-common/registry/bind"
	log "github.com/blackbeans/log4go"
	c "github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/codec"
	"github.com/blackbeans/turbo/packet"
	"github.com/blackbeans/turbo/pipe"
)

func (self *KiteClientManager) NodeChange(path string, eventType registry.RegistryEvent, children []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, registry.KITEQ_SERVER) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.WarnLog("kite_client", "KiteClientManager|ChildWatcher|INVALID SERVER PATH |%s|%t\n", path, children)
			return
		}
		//获取topic
		topic := split[3]
		log.WarnLog("kite_client", "KiteClientManager|ChildWatcher|Change|%s|%v|%+v", path, children, eventType)
		//search topic
		for _, t := range self.topics {
			if t == topic {
				self.onQServerChanged(topic, children)
				break
			}
		}
	}
}

//当触发QServer地址发生变更
func (self *KiteClientManager) onQServerChanged(topic string, hosts []string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	//重建一下topic下的kiteclient
	clients := make([]*kiteClient, 0, 10)
	for _, host := range hosts {
		//如果能查到remoteClient 则直接复用
		remoteClient := self.clientManager.FindRemoteClient(host)
		if nil == remoteClient {
			//这里就新建一个remote客户端连接
			conn, err := dial(host)
			if nil != err {
				log.ErrorLog("kite_client", "KiteClientManager|onQServerChanged|Create REMOTE CLIENT|FAIL|%s|%s\n", err, host)
				continue
			}
			remoteClient = c.NewRemotingClient(conn, func() codec.ICodec {
				return codec.LengthBasedCodec{
					MaxFrameLength: packet.MAX_PACKET_BYTES,
					SkipLength:     4}
			},
				func(rc *c.RemotingClient, p *packet.Packet) {
					event := pipe.NewPacketEvent(rc, p)
					err := self.pipeline.FireWork(event)
					if nil != err {
						log.ErrorLog("kite_client", "KiteClientManager|onPacketRecieve|FAIL|%s|%t\n", err, p)
					}
				}, self.rc)
			remoteClient.Start()
			auth, err := handshake(self.ga, remoteClient)
			if !auth || nil != err {
				remoteClient.Shutdown()
				log.ErrorLog("kite_client", "KiteClientManager|onQServerChanged|HANDSHAKE|FAIL|%s|%s\n", err, auth)
				continue
			}
			self.clientManager.Auth(self.ga, remoteClient)
		}

		//创建kiteClient
		kiteClient := newKitClient(remoteClient)
		clients = append(clients, kiteClient)
	}

	log.InfoLog("kite_client", "KiteClientManager|onQServerChanged|SUCC|%s|%s\n", topic, hosts)

	//替换掉线的server
	old, ok := self.kiteClients[topic]
	self.kiteClients[topic] = clients
	if ok {
		del := make([]string, 0, 2)
	outter:
		for _, o := range old {
			//决定删除的时候必须把所有的当前对应的client遍历一遍不然会删除掉
			for _, clients := range self.kiteClients {
				for _, c := range clients {
					if c.remotec.RemoteAddr() == o.remotec.RemoteAddr() {
						continue outter
					}
				}
			}
			del = append(del, o.remotec.RemoteAddr())
		}

		if len(del) > 0 {
			self.clientManager.DeleteClients(del...)
		}
	}
}

func (self *KiteClientManager) DataChange(path string, binds []*bind.Binding) {
	//IGNORE
	log.InfoLog("kite_client", "KiteClientManager|DataChange|%s|%s\n", path, binds)
}

func (self *KiteClientManager) OnSessionExpired() {
	//推送订阅关系和topics
	self.Start()

	log.InfoLog("kite_client", "KiteClientManager|OnSessionExpired|Restart...")
}
