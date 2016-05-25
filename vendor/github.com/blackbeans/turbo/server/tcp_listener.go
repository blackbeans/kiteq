package server

import (
	"errors"
	"net"
	"time"
)

var CONN_ERROR error = errors.New("STOP LISTENING")

//远程的listener
type StoppedListener struct {
	*net.TCPListener
	stop      chan bool
	keepalive time.Duration
}

//accept
func (self *StoppedListener) Accept() (*net.TCPConn, error) {
	for {
		conn, err := self.AcceptTCP()
		select {
		case <-self.stop:
			return nil, CONN_ERROR
		default:
			//do nothing
		}

		if nil == err {
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(self.keepalive)
		} else {
			return nil, err
		}

		return conn, err
	}
}
