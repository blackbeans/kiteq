package exchange

import (
	"context"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/logx"
	"github.com/blackbeans/turbo"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	DEFAULT_WARTER_MARK = int32(6000)
)

var log = logx.GetLogger("kiteq_server")

//用于管理订阅关系，对接zookeeper的订阅关系变更
type BindExchanger struct {
	exchanger      map[string] /*topic*/ map[string] /*groupId*/ []*registry.Binding  //保存的订阅关系
	limiters       map[string] /*topic*/ map[string] /*groupId*/ *turbo.BurstyLimiter //group->topic->limiter
	topics         []string                                                           //当前服务器可投递的topic类型
	lock           sync.RWMutex
	registryCenter *registry.RegistryCenter
	kiteqserver    string
	defaultLimiter *turbo.BurstyLimiter
}

func NewBindExchanger(parent context.Context, registryUri string,
	kiteQServer string) *BindExchanger {
	ex := &BindExchanger{
		exchanger: make(map[string]map[string][]*registry.Binding, 100),
		limiters:  make(map[string]map[string]*turbo.BurstyLimiter, 100),
		topics:    make([]string, 0, 50)}
	center := registry.NewRegistryCenter(parent, registryUri)
	center.RegisterWatcher(ex)

	//center.RegisterWatcher(PATH_SERVER, ex)
	//center.RegisterWatcher(PATH_SUB, ex)
	ex.registryCenter = center
	ex.kiteqserver = kiteQServer
	limiter, err := turbo.NewBurstyLimiter(int(DEFAULT_WARTER_MARK/2), int(DEFAULT_WARTER_MARK))
	if nil != err {
		panic(err)
	}
	ex.defaultLimiter = limiter
	return ex
}

//topics limiter
func (self *BindExchanger) Topic2Limiters() map[string]map[string][]int {
	wrapper := make(map[string]map[string][]int, 2)
	self.lock.RLock()
	defer self.lock.RUnlock()
	for t, m := range self.limiters {
		wrapper[t] = make(map[string][]int, 2)
		for g, l := range m {
			val := make([]int, 0, 2)
			acquried, total := l.LimiterInfo()
			val = append(val, acquried)
			val = append(val, total)
			wrapper[t][g] = val
		}
	}
	return wrapper
}

//当前topic到Groups的对应关系
func (self *BindExchanger) Topic2Groups() map[string][]string {
	binds := make(map[string][]string, 10)
	for topic, groups := range self.exchanger {
		v, ok := binds[topic]
		if !ok {
			v = make([]string, 0, len(groups))
		}

		for g, _ := range groups {
			v = append(v, g)
		}
		binds[topic] = v
	}
	return binds
}

//推送Qserver到配置中心
func (self *BindExchanger) PushQServer(hostport string, topics []string) bool {
	err := self.registryCenter.PublishQServer(hostport, topics)
	if nil != err {
		log.Errorf("BindExchanger|PushQServer|FAIL|%s|%s|%s", err, hostport, topics)
		return false
	}

	//删除掉不需要的topics
	delTopics := make([]string, 0, 2)
	for _, t := range self.topics {
		exist := false
		for _, v := range topics {
			if v == t {
				exist = true
				break
			}
		}
		//已经删除的topics
		if !exist {
			delTopics = append(delTopics, t)
		}
	}
	//存在需要删除的topics
	if len(delTopics) > 0 {
		self.registryCenter.UnPublishQServer(hostport, delTopics)
		func() {
			self.lock.Lock()
			defer self.lock.Unlock()
			for _, t := range delTopics {
				//清除掉对应的topics
				delete(self.exchanger, t)
			}
		}()
		log.Infof("BindExchanger|UnpushlishQServer|SUCC|%s|%s", hostport, delTopics)
	}

	//处理新增topic
	addedTopics := make([]string, 0, 2)
	for _, t := range topics {
		exist := false
		for _, v := range self.topics {
			if v == t {
				exist = true
				break
			}
		}
		//不存在则是新增的
		if !exist {
			addedTopics = append(addedTopics, t)
		}
	}
	sort.Strings(topics)
	func() {
		self.lock.Lock()
		defer self.lock.Unlock()
		self.topics = topics
	}()
	//订阅订阅关系变更
	succ := self.subscribeBinds(addedTopics)
	log.Infof("BindExchanger|PushQServer|SUCC|%s|%s", hostport, topics)
	return succ
}

//监听topics的对应的订阅关系的变更
func (self *BindExchanger) subscribeBinds(topics []string) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, topic := range topics {
		binds, err := self.registryCenter.GetBindAndWatch(topic)
		if nil != err {
			log.Errorf("BindExchanger|SubscribeBinds|FAIL|%s|%s", err, topic)
			return false
		} else {
			for groupId, bs := range binds {
				self.OnBindChanged(topic, groupId, bs)
				log.Infof("BindExchanger|SubscribeBinds|SUCC|%s|%s", topic, binds)
			}
		}
	}

	return true
}

//根据topic和messageType 类型获取订阅关系
func (self *BindExchanger) FindBinds(topic string, messageType string, filter func(b *registry.Binding) bool) ([]*registry.Binding, map[string]*turbo.BurstyLimiter) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	groups, ok := self.exchanger[topic]
	if !ok {
		return []*registry.Binding{}, nil
	}

	topicLimiters, ok := self.limiters[topic]
	limiters := make(map[string]*turbo.BurstyLimiter, 10)
	//符合规则的binds
	validBinds := make([]*registry.Binding, 0, 10)
	for _, binds := range groups {
		for _, b := range binds {
			//匹配并且不被过滤
			if b.Matches(topic, messageType) && !filter(b) {
				validBinds = append(validBinds, b)
				if ok {
					limiter, gok := topicLimiters[b.GroupId]
					if gok {
						limiters[b.GroupId] = limiter
					} else {
						//this is a bug
					}
				} else {
					//this is a bug
				}
			}
		}
	}

	return validBinds, limiters
}

//订阅关系改变
func (self *BindExchanger) OnBindChanged(topic, groupId string, newbinds []*registry.Binding) {

	if len(groupId) <= 0 {
		delete(self.exchanger, topic)
		return
	}

	//不是当前服务可以处理的topic则直接丢地啊哦
	if sort.SearchStrings(self.topics, topic) == len(self.topics) {
		log.Warnf("BindExchanger|onBindChanged|UnAccept Bindings|%s|%s|%s", topic, self.topics, newbinds)
		return
	}

	v, ok := self.exchanger[topic]

	if !ok {
		v = make(map[string][]*registry.Binding, 10)
		self.exchanger[topic] = v
	}

	limiter, lok := self.limiters[topic]
	if !lok {
		limiter = make(map[string]*turbo.BurstyLimiter, 10)
		self.limiters[topic] = limiter
	}

	if len(newbinds) > 0 {
		v[groupId] = newbinds

		//create limiter for topic group
		waterMark := newbinds[0].Watermark
		if waterMark <= 0 {
			waterMark = DEFAULT_WARTER_MARK
		}

		waterMark = int32(math.Min(float64(waterMark), float64(DEFAULT_WARTER_MARK)))

		li, liok := limiter[groupId]
		if !liok || ((int32)(li.PermitsPerSecond()) != waterMark) {
			lim, err := turbo.NewBurstyLimiter(int(waterMark/2), int(waterMark))
			if nil != err {
				log.Errorf("BindExchanger|onBindChanged|NewBurstyLimiter|FAIL|%v|%v|%v|%v", err, topic, groupId, waterMark)
				lim = self.defaultLimiter
			}
			limiter[groupId] = lim
		}
	} else {
		delete(v, groupId)
		delete(limiter, groupId)

	}
}

//当QServer变更
func (self *BindExchanger) OnQServerChanged(topic string, hosts []string) {

}

//当zk断开链接时
func (self *BindExchanger) OnSessionExpired() {
	err := self.registryCenter.PublishQServer(self.kiteqserver, self.topics)
	if nil != err {
		log.Errorf("BindExchanger|OnSessionExpired|PushQServer|FAIL|%s|%s|%s", err, self.kiteqserver, self.topics)
		return
	}

	//订阅订阅关系变更
	succ := self.subscribeBinds(self.topics)
	log.Infof("BindExchanger|OnSessionExpired|SUCC|subscribeBinds|%v|%s|%s", succ, self.kiteqserver, self.topics)

}

//关闭掉exchanger
func (self *BindExchanger) Shutdown() {
	//删除掉当前的QServer
	self.registryCenter.UnPublishQServer(self.kiteqserver, self.topics)
	time.Sleep(10 * time.Second)
	self.registryCenter.Close()
	log.Infof("BindExchanger|Shutdown...")
}
