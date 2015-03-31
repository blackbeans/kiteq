package mysql

import (
	"container/list"
	"database/sql"
	"errors"
	log "github.com/blackbeans/log4go"
	"sync"
	"time"
)

//连接工厂
type IStmtFactory interface {
	Get() (error, *sql.Stmt)            //获取一个stmt
	Release(stmt *sql.Stmt) error       //释放对应的stmt
	ReleaseBroken(stmt *sql.Stmt) error //释放掉坏的stmt
	Shutdown()                          //关闭当前的池子
	MonitorPool() (int, int, int)
}

//StmtPool的连接池
type StmtPool struct {
	dialFunc     func() (error, *sql.Stmt)
	maxPoolSize  int           //最大尺子大小
	minPoolSize  int           //最小连接池大小
	corepoolSize int           //核心池子大小
	numActive    int           //当前正在存活的client
	numWork      int           //当前正在工作的client
	idletime     time.Duration //空闲时间

	idlePool *list.List //空闲连接

	running bool

	mutex sync.Mutex //全局锁
}

type IdleStmt struct {
	stmt        *sql.Stmt
	expiredTime time.Time
}

func NewStmtPool(minPoolSize, corepoolSize,
	maxPoolSize int, idletime time.Duration,
	dialFunc func() (error, *sql.Stmt)) (error, *StmtPool) {

	idlePool := list.New()
	pool := &StmtPool{
		maxPoolSize:  maxPoolSize,
		corepoolSize: corepoolSize,
		minPoolSize:  minPoolSize,
		idletime:     idletime,
		idlePool:     idlePool,
		dialFunc:     dialFunc,
		running:      true}

	err := pool.enhancedPool(pool.minPoolSize)
	if nil != err {
		return err, nil
	}

	//启动链接过期
	go pool.evict()

	return nil, pool
}

func (self *StmtPool) enhancedPool(size int) error {

	//初始化一下最小的Poolsize,让入到idlepool中
	for i := 0; i < size; i++ {
		j := 0
		var err error
		var stmt *sql.Stmt
		for ; j < 3; j++ {
			err, stmt = self.dialFunc()
			if nil != err {
				log.Error("POOL_FACTORY|CREATE STMT|INIT|FAIL|%s\n", err)

			} else {
				break
			}
		}

		if j >= 3 {
			return errors.New("POOL_FACTORY|CREATE STMT|INIT|FAIL|%s" + err.Error())
		}

		idlestmt := &IdleStmt{stmt: stmt, expiredTime: (time.Now().Add(self.idletime))}
		self.idlePool.PushFront(idlestmt)
		self.numActive++
	}

	return nil
}

func (self *StmtPool) evict() {
	for self.running {

		select {
		case <-time.After(self.idletime):
			self.mutex.Lock()
			for e := self.idlePool.Back(); nil != e; e = e.Prev() {
				idlestmt := e.Value.(*IdleStmt)
				//如果当前时间在过期时间之后并且活动的链接大于corepoolsize则关闭
				isExpired := idlestmt.expiredTime.Before(time.Now())
				if isExpired &&
					self.numActive >= self.corepoolSize {
					idlestmt.stmt.Close()
					idlestmt = nil
					self.idlePool.Remove(e)
					//并且该表当前的active数量
					self.numActive--
				} else if isExpired {
					//过期的但是已经不够corepoolsize了直接重新设置过期时间
					idlestmt.expiredTime = time.Now().Add(self.idletime)
				} else {
					//活动的数量小于corepool的则修改存活时间
					idlestmt.expiredTime = time.Now().Add(self.idletime)
				}
			}
			self.mutex.Unlock()
		}
	}
}

func (self *StmtPool) MonitorPool() (int, int, int) {
	return self.numWork, self.numActive, self.idlePool.Len()
}

func (self *StmtPool) Get() (error, *sql.Stmt) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if !self.running {
		return errors.New("POOL_FACTORY|POOL IS SHUTDOWN"), nil
	}

	var stmt *sql.Stmt
	var err error
	//先从Idealpool中获取如果存在那么就直接使用
	if self.idlePool.Len() > 0 {
		e := self.idlePool.Back()
		idle := e.Value.(*IdleStmt)
		self.idlePool.Remove(e)
		stmt = idle.stmt
	}

	//如果当前依然是stmt
	if nil == stmt {
		//只有当前活动的链接小于最大的则创建
		if self.numActive < self.maxPoolSize {
			//如果没有可用链接则创建一个
			err, stmt = self.dialFunc()
			if nil != err {
				stmt = nil
			} else {
				self.numActive++
			}
		} else {
			return errors.New("POOLFACTORY|POOL|FULL!"), nil
		}
	}

	if nil != stmt {
		self.numWork++
	}

	return err, stmt
}

//释放坏的资源
func (self *StmtPool) ReleaseBroken(conn *sql.Stmt) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if nil != conn {
		conn.Close()
		conn = nil
	}

	var err error
	//只有当前的存活链接和当前工作链接大于0的时候才会去销毁
	if self.numActive > 0 && self.numWork > 0 {
		self.numWork--
		self.numActive--

	} else {
		err = errors.New("POOL|RELEASE BROKEN|INVALID CONN")
	}

	//判断当前是否连接不是最小连接
	incrCount := self.minPoolSize - self.numActive
	if incrCount < 0 {
		//如果不够最小连接则创建
		err = self.enhancedPool(incrCount)
	}

	return err
}

/**
* 归还当前的连接
**/
func (self *StmtPool) Release(stmt *sql.Stmt) error {

	idleStmt := &IdleStmt{stmt: stmt, expiredTime: (time.Now().Add(self.idletime))}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.numWork > 0 {
		//放入ideal池子中
		self.idlePool.PushFront(idleStmt)
		//工作链接数量--
		self.numWork--
		return nil
	} else {
		stmt.Close()
		stmt = nil
		log.Error("POOL|RELEASE|FAIL|%d\n", self.numActive)
		return errors.New("POOL|RELEASE|INVALID CONN")
	}

}

func (self *StmtPool) Shutdown() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.running = false

	for i := 0; i < 3; {
		//等待五秒中结束
		time.Sleep(5 * time.Second)
		if self.numWork <= 0 {
			break
		}

		log.Info("Statment Pool|CLOSEING|WORK POOL SIZE|:%d\n", self.numWork)
		i++
	}

	var idleStmt *IdleStmt
	//关闭掉空闲的client
	for e := self.idlePool.Front(); e != nil; e = e.Next() {
		idleStmt = e.Value.(*IdleStmt)
		idleStmt.stmt.Close()
		self.idlePool.Remove(e)
		idleStmt = nil
	}

	log.Info("Statment Pool|SHUTDOWN")
}
