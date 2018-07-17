package turbo

type IEvent interface {
}

//返回的事件
type IBackwardEvent interface {
	IEvent
}

//向前的事件
type IForwardEvent interface {
	IEvent
}

type PacketEvent struct {
	IForwardEvent
	Packet       *Packet //本次的数据包
	RemoteClient *TClient
}

func NewPacketEvent(remoteClient *TClient, packet *Packet) *PacketEvent {
	return &PacketEvent{Packet: packet, RemoteClient: remoteClient}
}

type HeartbeatEvent struct {
	IForwardEvent
	RemoteClient *TClient
	Opaque       int32
	Version      int64
}

//心跳事件
func NewHeartbeatEvent(remoteClient *TClient, opaque int32, version int64) *HeartbeatEvent {
	return &HeartbeatEvent{
		Version:      version,
		Opaque:       opaque,
		RemoteClient: remoteClient}

}

//远程操作事件
type RemotingEvent struct {
	Event      IForwardEvent
	futures    chan map[string]*Future //所有的回调的future
	errFutures map[string]*Future      //错误的回调future
	TargetHost []string                      //发送的特定hostport
	GroupIds   []string                      //本次发送的分组
	Packet     *Packet                //tlv的packet数据

}

func NewRemotingEvent(packet *Packet, targetHost []string, groupIds ...string) *RemotingEvent {
	revent := &RemotingEvent{
		TargetHost: targetHost,
		GroupIds:   groupIds,
		Packet:     packet,
		futures:    make(chan map[string]*Future, 1)}
	return revent
}

func (self *RemotingEvent) AttachEvent(Event IForwardEvent) {
	self.Event = Event
}

func (self *RemotingEvent) AttachErrFutures(futures map[string]*Future) {
	self.errFutures = futures
}

//等待响应
func (self *RemotingEvent) Wait() map[string]*Future {
	return <-self.futures
}

//网络回调事件
type RemoteFutureEvent struct {
	IBackwardEvent
	*RemotingEvent
	Futures map[string] /*groupid*/ *Future //网络结果回调
}

//网络回调事件
func NewRemoteFutureEvent(remoteEvent *RemotingEvent, futures map[string]*Future) *RemoteFutureEvent {
	fe := &RemoteFutureEvent{Futures: futures}
	fe.RemotingEvent = remoteEvent
	return fe
}

//到头的事件
type SunkEvent struct {
	IForwardEvent
}
