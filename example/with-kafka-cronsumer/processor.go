package main

import (
	"errors"
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-konsumer"
	"time"
)

type messageProcessor struct {
	cronsumer kcronsumer.Cronsumer
}

type Processor interface {
	kafka.Processor
	Start()
	Stop()
}

var _ kafka.Processor = (*messageProcessor)(nil)

func NewProcessor() Processor {
	p := &messageProcessor{}

	p.cronsumer = cronsumer.New(&kcronsumer.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kcronsumer.ConsumerConfig{
			GroupID:     "standart-cg",
			Topic:       "exception-topic",
			Concurrency: 1,
			Cron:        "*/1 * * * *",
			Duration:    50 * time.Second,
		},
		LogLevel: "info",
	}, p.Consume)

	return p
}

func (m *messageProcessor) Start() {
	m.cronsumer.Start()
}

func (m *messageProcessor) Stop() {
	m.cronsumer.Stop()
}

func (m *messageProcessor) Process(message kafka.Message) {
	headers := make([]kcronsumer.Header, 0, len(message.Headers))
	for i := range headers {
		headers = append(headers, kcronsumer.Header{
			Key:   headers[i].Key,
			Value: headers[i].Value,
		})
	}

	msg := kcronsumer.NewMessageBuilder().
		WithKey(message.Key).
		WithValue(message.Value).
		WithTopic(message.Topic).
		WithHeaders(headers).
		WithPartition(message.Partition).
		WithHighWatermark(message.HighWaterMark).
		Build()

	err := m.Consume(msg)
	if err != nil {
		msg.Topic = "exception-topic"
		if err = m.cronsumer.Produce(msg); err != nil {
			fmt.Printf("Message could not be sent to exception topic. message: %s error: %v \n", string(message.Value), err)
		}
	}
}

func (m *messageProcessor) Consume(ms kcronsumer.Message) error {
	fmt.Println(string(ms.Value))
	return errors.New("deneme")
}
