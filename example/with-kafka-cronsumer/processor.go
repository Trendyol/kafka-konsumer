package main

import (
	"errors"
	"fmt"
	"github.com/Abdulsametileri/kafka-template/pkg/listener"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
	"time"
)

var _ listener.MessageProcessor = (*messageProcessor)(nil)

type messageProcessor struct {
	Cronsumer          kcronsumer.Cronsumer
	exceptionTopicName string
}

func NewProcessor() *messageProcessor {
	processor := &messageProcessor{
		exceptionTopicName: "exception-topic",
	}

	processor.Cronsumer = cronsumer.New(&kcronsumer.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "standart-cg",
			Topic:    "exception-topic",
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
		},
	}, processor.Consume)

	return processor
}

func (m *messageProcessor) Process(message segmentio.Message) {
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

	if err := m.Consume(msg); err != nil {
		msg.Topic = m.exceptionTopicName
		if err = m.Cronsumer.Produce(msg); err != nil {
			fmt.Errorf(fmt.Sprintf("Message could not be sent to exception topic. message: %s error: %v", string(message.Value), err))
		}
	}
}

func (m *messageProcessor) Consume(message kcronsumer.Message) error {
	fmt.Println(message)
	return errors.New("dewneme")
}
