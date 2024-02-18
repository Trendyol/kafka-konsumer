package main

import (
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
	"time"
)

func main() {
	firstConsumer, _ := kafka.NewConsumer(&kafka.ConsumerConfig{
		MetricPrefix: "first",
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "standart-topic",
			GroupID: "standart-cg",
		},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			MetricPrefix:  "first",
			Brokers:       []string{"localhost:29092"},
			Topic:         "error-topic",
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  50 * time.Second,
			MaxRetry:      3,
		},
		ConsumeFn: consumeFn,
	})
	defer firstConsumer.Stop()
	firstConsumer.Consume()

	secondConsumer, _ := kafka.NewConsumer(&kafka.ConsumerConfig{
		MetricPrefix: "second",
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "another-standart-topic",
			GroupID: "another-standart-cg",
		},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			MetricPrefix:  "second",
			Brokers:       []string{"localhost:29092"},
			Topic:         "retry-topic",
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  50 * time.Second,
			MaxRetry:      3,
		},
		ConsumeFn: consumeFn,
	})
	defer secondConsumer.Stop()

	secondConsumer.Consume()

	allCollectors := append(firstConsumer.GetMetricCollectors(), secondConsumer.GetMetricCollectors()...)
	StartAPI(allCollectors...)

	select {}
}

func consumeFn(message *kafka.Message) error {
	fmt.Printf("Message From %s with value %s\n", message.Topic, string(message.Value))
	return nil
}
