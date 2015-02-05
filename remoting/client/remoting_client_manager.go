package client

import (
	// "log"
	"sync"
)

//群组授权信息
type GroupAuth struct {
	SecretKey, GroupId string
}

func NewGroupAuth(secretKey, groupId string) *GroupAuth {
	return &GroupAuth{SecretKey: secretKey, GroupId: groupId}
}

//远程client管理器
type ClientManager struct {
	reconnectManager *ReconnectManager
	groupAuth        map[string] /*host:port*/ *GroupAuth
	groupClients     map[string] /*groupId*/ []*RemotingClient
	allClients       map[string] /*host:port*/ *RemotingClient
	lock             sync.Mutex
}

func NewClientManager(reconnectManager *ReconnectManager) *ClientManager {
	return &ClientManager{
		groupAuth:        make(map[string]*GroupAuth, 10),
		groupClients:     make(map[string][]*RemotingClient, 50),
		allClients:       make(map[string]*RemotingClient, 100),
		reconnectManager: reconnectManager}
}

func (self *ClientManager) ClientsClone() map[string]*RemotingClient {
	self.lock.Lock()
	defer self.lock.Unlock()

	clone := make(map[string]*RemotingClient, len(self.allClients))
	for k, v := range self.allClients {
		clone[k] = v
	}

	return clone
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

func (self *ClientManager) SubmitReconnect(c *RemotingClient) {
	ga, ok := self.groupAuth[c.RemoteAddr()]
	if ok {
		self.reconnectManager.submit(newReconnectTasK(c, ga))
	}
}

//查找remotingclient
func (self *ClientManager) FindRemoteClient(hostport string) *RemotingClient {
	self.lock.Lock()
	defer self.lock.Unlock()
	// log.Printf("ClientManager|FindRemoteClient|%s|%s\n", hostport, self.allClients)
	rclient, ok := self.allClients[hostport]
	if !ok || rclient.IsClosed() {
		return nil
	} else {

	}
	return rclient
}

//查找匹配的groupids
func (self *ClientManager) FindRemoteClients(groupIds []string, filter func(groupId string) bool) map[string][]*RemotingClient {
	self.lock.Lock()
	defer self.lock.Unlock()

	clients := make(map[string][]*RemotingClient, 10)
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
				//提交到重连
				self.SubmitReconnect(c)
				continue
			}

			//如果当前client处于非关闭状态并且没有过滤则入选
			if !filter(gid) {
				// log.Println("find a client", client.groupId)
				gclient = append(gclient, c)
			}

		}
		clients[gid] = gclient
	}
	// log.Println("Find clients result ", clients)
	return clients
}

func (self *ClientManager) Shutdown() {
	for _, c := range self.allClients {
		c.Shutdown()
	}
}
