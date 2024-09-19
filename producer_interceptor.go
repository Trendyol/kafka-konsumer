package kafka

import (
	"context"
)

type ProducerInterceptorContext struct {
	Context context.Context
	Message *Message
}

type ProducerInterceptor interface {
	OnProduce(ctx ProducerInterceptorContext)
}
