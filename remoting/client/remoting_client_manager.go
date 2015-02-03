package client

import (
	// "log"
	"sync"
)

//远程client管理器
type ClientManager struct {
	groupClients map[string] /*groupId*/ []*RemotingClient
	allClients   map[string] /*host:port*/ *RemotingClient
	lock         sync.Mutex
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		groupClients: make(map[string][]*RemotingClient, 50),
		allClients:   make(map[string]*RemotingClient, 100)}
}

func (self *ClientManager) Add(groupId string, remoteClient *RemotingClient) {
	self.lock.Lock()
	defer self.lock.Unlock()

	cs, ok := self.groupClients[groupId]
	if !ok {
		cs = make([]*RemotingClient, 0, 50)
	}

	//创建remotingClient
	self.groupClients[groupId] = append(cs, remoteClient)
	self.allClients[remoteClient.RemoteAddr()] = remoteClient
}

//查找remotingclient
func (self *ClientManager) FindRemoteClient(hostport string) *RemotingClient {
	self.lock.Lock()
	defer self.lock.Unlock()
	// log.Printf("ClientManager|FindRemoteClient|%s|%s\n", hostport, self.allClients)
	rclient, _ := self.allClients[hostport]
	return rclient
}

//查找匹配的groupids
func (self *ClientManager) FindRemoteClients(groupIds []string, filter func(groupId string) bool) map[string][]*RemotingClient {
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

		for _, client := range self.groupClients[gid] {
			if !filter(client.remoteSession.GroupId) {
				// log.Println("find a client", client.groupId)
				gclient = append(gclient, client)
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
