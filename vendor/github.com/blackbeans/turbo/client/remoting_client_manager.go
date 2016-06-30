package client

import (
	log "github.com/blackbeans/log4go"
	"sort"
	"sync"
	"time"
)

//群组授权信息
type GroupAuth struct {
	GroupId, SecretKey string
	authtime           int64
}

func NewGroupAuth(groupId, secretKey string) *GroupAuth {
	return &GroupAuth{SecretKey: secretKey, GroupId: groupId, authtime: time.Now().Unix()}
}

//远程client管理器
type ClientManager struct {
	reconnectManager *ReconnectManager
	groupAuth        map[string] /*host:port*/ *GroupAuth
	groupClients     map[string] /*groupId*/ []*RemotingClient
	allClients       map[string] /*host:port*/ *RemotingClient
	lock             sync.RWMutex
}

func NewClientManager(reconnectManager *ReconnectManager) *ClientManager {

	cm := &ClientManager{
		groupAuth:        make(map[string]*GroupAuth, 10),
		groupClients:     make(map[string][]*RemotingClient, 50),
		allClients:       make(map[string]*RemotingClient, 100),
		reconnectManager: reconnectManager}
	go cm.evict()
	return cm
}

func (self *ClientManager) evict() {
	log.Info("ClientManager|evict...")
	tick := time.NewTicker(1 * time.Minute)
	for {
		clients := self.ClientsClone()
		for _, c := range clients {
			if c.IsClosed() {
				//可能会删除连接，如果不开启重连策略的话
				self.SubmitReconnect(c)
			}
		}
		<-tick.C
	}
}

//connection numbers
func (self *ClientManager) ConnNum() int32 {
	i := int32(0)
	clients := self.ClientsClone()
	for _, c := range clients {
		if !c.IsClosed() {
			i++
		}
	}
	return i
}

//验证是否授权
func (self *ClientManager) Validate(remoteClient *RemotingClient) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	_, auth := self.groupAuth[remoteClient.RemoteAddr()]
	return auth
}

func (self *ClientManager) Auth(auth *GroupAuth, remoteClient *RemotingClient) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	cs, ok := self.groupClients[auth.GroupId]
	if !ok {
		cs = make([]*RemotingClient, 0, 50)
	}
	//创建remotingClient
	self.groupClients[auth.GroupId] = append(cs, remoteClient)
	self.allClients[remoteClient.RemoteAddr()] = remoteClient
	self.groupAuth[remoteClient.RemoteAddr()] = auth
	return true
}

func (self *ClientManager) ClientsClone() map[string]*RemotingClient {
	self.lock.RLock()
	defer self.lock.RUnlock()
	clone := make(map[string]*RemotingClient, len(self.allClients))
	for k, v := range self.allClients {
		clone[k] = v
	}
	return clone
}

//返回group到IP的对应关系
func (self *ClientManager) CloneGroups() map[string][]string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	clone := make(map[string][]string, len(self.groupClients))
	for k, v := range self.groupClients {
		clients := make([]string, 0, len(v))
		for _, c := range v {
			clients = append(clients, c.RemoteAddr())
		}
		sort.Strings(clients)
		clone[k] = clients
	}
	return clone
}

func (self *ClientManager) DeleteClients(hostports ...string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, hostport := range hostports {
		//取消重连
		self.removeClient(hostport)
		//取消重连任务
		self.reconnectManager.cancel(hostport)

	}
}

func (self *ClientManager) removeClient(hostport string) {
	ga, ok := self.groupAuth[hostport]
	if ok {
		//删除分组
		delete(self.groupAuth, hostport)
		//删除group中的client
		gc, ok := self.groupClients[ga.GroupId]
		if ok {
			for i, cli := range gc {
				if cli.RemoteAddr() == hostport {
					self.groupClients[ga.GroupId] = append(gc[0:i], gc[i+1:]...)
					break
				}
			}
		}

		//删除hostport->client的对应关系
		c, ok := self.allClients[hostport]
		if ok {
			c.Shutdown()
			delete(self.allClients, hostport)
		}
	}

	log.Info("ClientManager|removeClient|%s...\n", hostport)
}

func (self *ClientManager) SubmitReconnect(c *RemotingClient) {

	self.lock.Lock()
	defer self.lock.Unlock()
	ga, ok := self.groupAuth[c.RemoteAddr()]
	//如果重连则提交重连任务,并且该分组存在该机器则重连
	if ok && self.reconnectManager.allowReconnect {

		self.reconnectManager.submit(c, ga, func(addr string) {
			//重连任务失败完成后的hook,直接移除该机器
			self.DeleteClients(addr)
		})
		return

	}

	//不需要重连的直接删除掉连接,或者分组不存在则直接删除
	self.removeClient(c.RemoteAddr())

}

//查找remotingclient
func (self *ClientManager) FindRemoteClient(hostport string) *RemotingClient {
	self.lock.RLock()
	defer self.lock.RUnlock()
	// log.Printf("ClientManager|FindRemoteClient|%s|%s\n", hostport, self.allClients)
	rclient, ok := self.allClients[hostport]
	if !ok || rclient.IsClosed() {
		//已经关闭的直接返回nil
		return nil
	}
	return rclient
}

//查找匹配的groupids
func (self *ClientManager) FindRemoteClients(groupIds []string, filter func(groupId string, rc *RemotingClient) bool) map[string][]*RemotingClient {
	clients := make(map[string][]*RemotingClient, 10)
	var closedClients []*RemotingClient
	self.lock.RLock()
	for _, gid := range groupIds {
		if len(self.groupClients[gid]) <= 0 {
			continue
		}
		//按groupId来获取remoteclient
		gclient, ok := clients[gid]
		if !ok {
			gclient = make([]*RemotingClient, 0, 10)
		}

		for _, c := range self.groupClients[gid] {

			if c.IsClosed() {
				if nil == clients {
					closedClients = make([]*RemotingClient, 0, 2)
				}
				closedClients = append(closedClients, c)
				continue
			}
			//如果当前client处于非关闭状态并且没有过滤则入选
			if !filter(gid, c) {
				gclient = append(gclient, c)
			}
		}
		clients[gid] = gclient
	}
	self.lock.RUnlock()

	//删除掉关掉的clients
	if nil != closedClients && len(closedClients) > 0 {
		for _, c := range closedClients {
			self.SubmitReconnect(c)
		}
	}

	// log.Printf("Find clients result |%s|%s\n", clients, self.groupClients)
	return clients
}

func (self *ClientManager) Shutdown() {
	self.reconnectManager.stop()
	for _, c := range self.allClients {
		c.Shutdown()
	}
	log.Info("ClientManager|Shutdown....")
}
