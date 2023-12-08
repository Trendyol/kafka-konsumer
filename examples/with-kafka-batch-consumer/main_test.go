package main

import (
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	"testing"
	"time"
)

func BenchmarkKonsumer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		consumerCfg := &kafka.ConsumerConfig{
			Reader: kafka.ReaderConfig{
				Brokers: []string{"localhost:29092"},
				Topic:   "standart-topic",
				GroupID: "standart-cg",
			},
			BatchConfiguration: &kafka.BatchConfiguration{
				MessageGroupLimit: 1000,
				BatchConsumeFn:    batchConsumeFn,
			},
			RetryEnabled: true,
			RetryConfiguration: kafka.RetryConfiguration{
				Brokers:       []string{"localhost:29092"},
				Topic:         "retry-topic",
				StartTimeCron: "*/1 * * * *",
				WorkDuration:  50 * time.Second,
				MaxRetry:      3,
			},
			MessageGroupDuration: time.Second,
		}

		consumer, _ := kafka.NewConsumer(consumerCfg)

		consumer.Consume()

		fmt.Println("Consumer started...!")

		time.Sleep(10 * time.Second)
	}
}
