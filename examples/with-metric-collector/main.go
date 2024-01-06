package main

import (
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
	"time"
)

func main() {
	firstConsumerCfg := &kafka.ConsumerConfig{
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
	}

	firstConsumer, _ := kafka.NewConsumer(firstConsumerCfg)
	defer firstConsumer.Stop()

	go firstConsumer.Consume()

	secondConsumerCfg := &kafka.ConsumerConfig{
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
	}

	secondConsumer, _ := kafka.NewConsumer(secondConsumerCfg)
	defer secondConsumer.Stop()

	go secondConsumer.Consume()

	allCollectors := append(firstConsumer.GetMetricCollectors(), secondConsumer.GetMetricCollectors()...)
	StartAPI(allCollectors...)

	select {}
}

func consumeFn(message *kafka.Message) error {
	fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))
	return nil
}
