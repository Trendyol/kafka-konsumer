package test

import "github.com/Trendyol/kafka-konsumer/v2"

type MockProducerInterceptor struct{}

const (
	XSourceAppKey   = "x-source-app"
	XSourceAppValue = "kafka-konsumer"
)

func (i *MockProducerInterceptor) OnProduce(ctx kafka.ProducerInterceptorContext) {
	ctx.Message.Headers = append(ctx.Message.Headers, kafka.Header{
		Key:   XSourceAppKey,
		Value: []byte(XSourceAppValue),
	})
}

func NewMockProducerInterceptor() kafka.ProducerInterceptor {
	return &MockProducerInterceptor{}
}
