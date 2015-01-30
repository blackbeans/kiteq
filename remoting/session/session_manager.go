package session

import (
	"errors"
)

//session管理器

const MAX_ESTABLISH_SESSIONS = 1000

type SessionManager struct {
	sessions map[string][]*Session //当前机器的session

}

func NewSesstionManager() {
	return &SessionManager{
		sessions: make(map[string][]*Session),
	}
}

func (self *SessionManager) Add(groupId string, session *Session) {
	if self.sessions[groupId] == nil {
		self.sessions = make([]*Session, 0, MAX_ESTABLISH_SESSIONS)
	}
	self.sessions = append(self.sessions, session)
}

func (self *SessionManager) Write(groupId string, packet []byte) error {
	if self.sessions[groupId] == nil || len(self.sessions[groupId]) == 0 {
		return errors.New("no session is available")
	}
	// 选取一个session投递
	session := self.sessions[groupId][0]
	session.WriteChannel <- packet
	return nil
}
