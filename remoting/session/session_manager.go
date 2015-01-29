package session

//session管理器

type SessionManager struct {
	sessions map[string][]*Session //当前机器的session

}

//查找匹配的groupids
func (self *SessionManager) FindSessions(groupIds []string, filter func(s *Session) bool) []*Session {
	return nil
}
