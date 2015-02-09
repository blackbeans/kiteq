package pipe

import (
	"errors"
)

//处理器
type IEventProcessor interface {
	Process(ctx *DefaultPipelineContext, event IEvent) error
	//事件类型判断
	TypeAssert(event IEvent) bool
}

//处理器接口
type IHandler interface {
	GetName() string //获得当前处理的handler名称

	HandleEvent(ctx *DefaultPipelineContext, event IEvent) error

	//检查是否可以处理改event
	AcceptEvent(event IEvent) bool
}

//处理向后的事件的handler
type IBackwardHandler interface {
	IHandler
	HandleBackward(ctx *DefaultPipelineContext, event IBackwardEvent) error
}

//处理向前的handler
type IForwardHandler interface {
	IHandler
	HandleForward(ctx *DefaultPipelineContext, event IForwardEvent) error
}

var ERROR_INVALID_EVENT_TYPE error = errors.New("ERROR INVALID EVENT TYPE")

//-------------基本的forward处理
type BaseForwardHandler struct {
	IForwardHandler
	processor IEventProcessor //类型判断的实现
	name      string
}

func NewBaseForwardHandler(name string, processor IEventProcessor) BaseForwardHandler {
	return BaseForwardHandler{
		name:      name,
		processor: processor}
}

func (self *BaseForwardHandler) GetName() string {
	return self.name
}

//检查是否可以处理改event
func (self *BaseForwardHandler) AcceptEvent(event IEvent) bool {
	//是否可以处理当前按的event，再去判断具体的可处理事件类型
	_, ok := event.(IForwardEvent)
	return ok
}

func (self *BaseForwardHandler) HandleForward(ctx *DefaultPipelineContext, event IForwardEvent) error {

	//处理逻辑成功则向后传递
	if !self.processor.TypeAssert(event) {
		ctx.SendForward(event)
		return nil
	} else {
		return self.processor.Process(ctx, event)
	}
}

func (self *BaseForwardHandler) HandleEvent(ctx *DefaultPipelineContext, event IEvent) error {
	return self.processor.Process(ctx, event)
}

//-------------基本的backward处理
type BaseBackwardHandler struct {
	IBackwardHandler
	processor IEventProcessor //类型判断的实现
	name      string
}

func NewBaseBackwardHandler(name string, processor IEventProcessor) BaseBackwardHandler {
	return BaseBackwardHandler{
		name:      name,
		processor: processor}
}

func (self *BaseBackwardHandler) GetName() string {
	return self.name
}

//检查是否可以处理改event
func (self *BaseBackwardHandler) AcceptEvent(event IEvent) bool {
	//是否可以处理当前按的event，再去判断具体的可处理事件类型
	_, ok := event.(IBackwardEvent)
	return ok
}

func (self *BaseBackwardHandler) HandleBackward(ctx *DefaultPipelineContext, event IBackwardEvent) error {

	//处理逻辑成功则向后传递
	if !self.processor.TypeAssert(event) {
		ctx.SendBackward(event)
		return nil
	} else {
		return self.processor.Process(ctx, event)
	}
}

func (self *BaseBackwardHandler) HandleEvent(ctx *DefaultPipelineContext, event IEvent) error {
	return self.processor.Process(ctx, event)
}
