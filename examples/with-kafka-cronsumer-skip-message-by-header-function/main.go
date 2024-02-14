package main

import (
	"fmt"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-konsumer/v2"
	"os"
	"os/signal"
	"time"
)

func main() {
	consumerCfg := &kafka.ConsumerConfig{
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "standart-topic",
			GroupID: "standart-cg",
		},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:               []string{"localhost:29092"},
			Topic:                 "retry-topic",
			StartTimeCron:         "*/1 * * * *",
			WorkDuration:          50 * time.Second,
			MaxRetry:              3,
			SkipMessageByHeaderFn: skipMessageByHeaderFn,
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

func consumeFn(message *kafka.Message) error {
	fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))
	return nil
}

func skipMessageByHeaderFn(headers []kcronsumer.Header) bool {
	for _, header := range headers {
		if header.Key == "skipMessage" {
			// If a kafka message comes with `skipMessage` header key, it will be skipped!
			return true
		}
	}
	return false
}
