package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	"os"
	"os/signal"
	"time"
)

const (
	topicName           = "standart-topic"
	retryTopicName      = "retry-topic"
	deadLetterTopicName = "error-topic"
)

func main() {
	producer, _ := kafka.NewProducer(kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	})

	_ = producer.Produce(context.Background(), kafka.Message{
		Topic: topicName,
		Key:   []byte("1"),
		Value: []byte(`{ "foo": "bar" }`),
	})

	consumerCfg := &kafka.ConsumerConfig{
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   topicName,
			GroupID: "konsumer.group.test",
		},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			DeadLetterTopic: deadLetterTopicName,
			Brokers:         []string{"localhost:29092"},
			Topic:           retryTopicName,
			StartTimeCron:   "*/1 * * * *",
			WorkDuration:    50 * time.Second,
			MaxRetry:        1,
		},
		ConsumeFn: consumeFn,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func consumeFn(message kafka.Message) error {
	fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))
	// returns error to be sent to dead-letter topic
	return errors.New("consumer error")
}
