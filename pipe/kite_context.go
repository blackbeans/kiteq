package pipe

import (
	"container/list"
	log "github.com/blackbeans/log4go"
)

/**
*
*本类主要管理当前注册的所有的事件对应的handler
* ---> forwardhandler--->  forwardhandler	---->
*											 		backward+forwardAdapterHandler
* <--- backwardhandler <----backward handler<----
*
*
**/

//-----------------整个Handler的pipeline 组成了一个循环链表
type DefaultPipeline struct {
	eventHandler     *list.List               //event对应的Handler的
	hashEventHandler map[string]*list.Element //hash对应的handler
}

//创建pipeline
func NewDefaultPipeline() *DefaultPipeline {
	return &DefaultPipeline{
		eventHandler:     list.New(),
		hashEventHandler: make(map[string]*list.Element)}
}

func (self *DefaultPipeline) FireWork(event IForwardEvent) error {
	ctx := self.getCtx()
	return ctx.handler.HandleEvent(ctx, event)
}

func (self *DefaultPipeline) getCtx() *DefaultPipelineContext {
	return self.eventHandler.Front().Value.(*DefaultPipelineContext)
}

func (self *DefaultPipeline) getNextContextByHandlerName(name string) *DefaultPipelineContext {
	ctx, ok := self.hashEventHandler[name]
	if ok {
		next := ctx.Next()
		if nil != next {
			return next.Value.(*DefaultPipelineContext)
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func (self *DefaultPipeline) getPreContextByHandlerName(name string) *DefaultPipelineContext {
	ctx, ok := self.hashEventHandler[name]
	if ok {
		pre := ctx.Prev()
		if nil != pre {
			return pre.Value.(*DefaultPipelineContext)
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func (self *DefaultPipeline) RegisteHandler(name string, handler IHandler) {
	_, canHandleForward := handler.(IForwardHandler)
	_, canHandleBackward := handler.(IBackwardHandler)

	ctx := &DefaultPipelineContext{handler: handler,
		canHandleForward:  canHandleForward,
		canHandleBackward: canHandleBackward}
	ctx.pipeline = self
	currctx := self.eventHandler.PushBack(ctx)
	self.hashEventHandler[name] = currctx
	log.Info("DefaultPipeline|RegisteHandler|%s\n", name)
}

//pipeline中处理向后的事件
func (self *DefaultPipeline) handleBackward(ctx *DefaultPipelineContext, event IBackwardEvent) error {
	backwardHandler := ctx.handler.(IBackwardHandler)
	return backwardHandler.HandleBackward(ctx, event)
}

//pipeline中处理向前的事件
func (self *DefaultPipeline) handleForward(ctx *DefaultPipelineContext, event IForwardEvent) error {
	forwardHandler := ctx.handler.(IForwardHandler)
	return forwardHandler.HandleForward(ctx, event)

}

//pipeline的尽头处理
func (self *DefaultPipeline) eventSunk(event IEvent) {

	// log.Info("DefaultPipeline|eventSunk|event:%t\n", event)
}

//pipeline处理中间出现错误
func (self *DefaultPipeline) errorCaught(event IEvent, err error) error {
	log.Info("DefaultPipeline|errorCaught|err:%s\n", err)
	return err
}

//------------------------当前Pipeline的上下文
type DefaultPipelineContext struct {
	handler           IHandler         //当前上下文的handler
	pipeline          *DefaultPipeline //默认的pipeline
	canHandleForward  bool             //能处理向前的
	canHandleBackward bool             //能处理向后的
}

//向后投递
func (self *DefaultPipelineContext) SendForward(event IForwardEvent) {
	actualCtx := self.getForwardContext(self, event)
	if nil == actualCtx {
		//已经没有处理的handler 找默认的handler处理即可
		self.pipeline.eventSunk(event)

	} else {

		err := actualCtx.pipeline.handleForward(actualCtx, event)
		//如果处理失败了则需要直接走exception的handler
		if nil != err {
			self.pipeline.errorCaught(event, err)
		}
	}
}

//获取forward的上下文
func (self *DefaultPipelineContext) getForwardContext(ctx *DefaultPipelineContext, event IForwardEvent) *DefaultPipelineContext {
	nextCtx := ctx.pipeline.getNextContextByHandlerName(ctx.handler.GetName())
	for nil != nextCtx {
		if nextCtx.canHandleForward &&
			nextCtx.handler.AcceptEvent(event) {
			return nextCtx
		} else {
			nextCtx = ctx.pipeline.getNextContextByHandlerName(nextCtx.handler.GetName())
		}

	}
	return nil
}

func (self *DefaultPipelineContext) SendBackward(event IBackwardEvent) {
	actualCtx := self.getBackwardContext(self, event)
	if nil == actualCtx {
		//已经没有处理的handler 找默认的handler处理即可
		self.pipeline.eventSunk(event)
	} else {
		err := actualCtx.pipeline.handleBackward(actualCtx, event)
		if nil != err {
			self.pipeline.errorCaught(event, err)
		}
	}
}

//获取backward对应的上线文
func (self *DefaultPipelineContext) getBackwardContext(ctx *DefaultPipelineContext, event IBackwardEvent) *DefaultPipelineContext {
	preCtx := ctx.pipeline.getPreContextByHandlerName(ctx.handler.GetName())
	for nil != preCtx {
		if preCtx.canHandleBackward &&
			preCtx.handler.AcceptEvent(event) {
			return preCtx
		} else {
			preCtx = ctx.pipeline.getPreContextByHandlerName(preCtx.handler.GetName())
		}
	}

	return nil
}
