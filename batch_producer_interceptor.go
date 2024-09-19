package kafka

import (
	"context"
)

type BatchProducerInterceptorContext struct {
	Context context.Context
	Message *Message
}

type BatchProducerInterceptor interface {
	OnProduce(ctx context.Context, msg Message)
}
