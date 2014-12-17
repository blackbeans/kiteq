package handler

import (
	"errors"
)

var ERROR_INVALID_EVENT_TYPE error = errors.New("invalid event type !")

//处理器接口
type IHandler interface {
	GetName() string //获得当前处理的handler名称

	//检查是否可以处理改event
	AcceptEvent(event IEvent) bool

	// 处理事件
	HandleEvent(ctx *DefaultPipelineContext, event IEvent) error
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
