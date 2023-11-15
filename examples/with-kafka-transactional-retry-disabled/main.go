package main

import (
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	"os"
	"os/signal"
	"time"
)

func main() {
	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "standart-topic",
			GroupID: "standart-cg",
		},
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit:    1000,
			MessageGroupDuration: time.Second,
			BatchConsumeFn:       batchConsumeFn,
		},
		RetryEnabled:       true,
		TransactionalRetry: kafka.NewBoolPtr(false),
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:       []string{"localhost:29092"},
			Topic:         "retry-topic",
			StartTimeCron: "*/5 * * * *",
			WorkDuration:  4 * time.Minute,
			MaxRetry:      3,
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

// In order to load topic with data, use:
// kafka-console-producer --broker-list localhost:29092 --topic standart-topic < examples/load.txt
func batchConsumeFn(messages []*kafka.Message) error {
	// you can add custom error handling here & flag messages
	for i := range messages {
		if i%2 == 0 {
			messages[i].IsFailed = true
		}
	}

	// you must return error here to retry only failed messages
	return errors.New("err")
}
