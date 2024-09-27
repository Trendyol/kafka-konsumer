package main

import "github.com/Trendyol/kafka-konsumer/v2"

type producerInterceptor struct{}

func (i *producerInterceptor) OnProduce(ctx kafka.ProducerInterceptorContext) {
	ctx.Message.Headers = append(ctx.Message.Headers, kafka.Header{
		Key:   "x-source-app",
		Value: []byte("kafka-konsumer"),
	})
}

func newProducerInterceptor() []kafka.ProducerInterceptor {
	return []kafka.ProducerInterceptor{&producerInterceptor{}}
}
