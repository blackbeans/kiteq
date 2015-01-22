package session

//session管理器

type SessionManager struct {
	sessions map[string][]*Session //当前机器的session

}
