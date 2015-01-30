package session

import (
// "log"
)

//session管理器

const MAX_ESTABLISH_SESSIONS = 1000

type SessionManager struct {
	sessions map[string][]*Session //当前机器的session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string][]*Session),
	}
}

func (self *SessionManager) Add(groupId string, session *Session) {
	if self.sessions[groupId] == nil {
		self.sessions[groupId] = make([]*Session, 0, MAX_ESTABLISH_SESSIONS)
	}
	self.sessions[groupId] = append(self.sessions[groupId], session)
}

//查找匹配的groupids
func (self *SessionManager) FindSessions(groupIds []string, filter func(s *Session) bool) []*Session {
	sessions := make([]*Session, 0, 1)
	for _, gid := range groupIds {
		if self.sessions[gid] == nil || len(self.sessions[gid]) == 0 {
			continue
		}
		for _, session := range self.sessions[gid] {
			if !filter(session) {
				// log.Println("find a session", session.GroupId)
				sessions = append(sessions, session)
			}
		}
	}
	// log.Println("Find sessions result ", sessions)
	return sessions
}
