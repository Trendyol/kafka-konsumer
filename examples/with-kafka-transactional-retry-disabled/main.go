package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
	"os"
	"os/signal"
	"time"
)

func main() {
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{Brokers: []string{"localhost:29092"}, Topic: "standart-topic"},
	})

	producer.ProduceBatch(context.Background(), []kafka.Message{
		{Key: []byte("key1"), Value: []byte("message1")},
		{Key: []byte("key2"), Value: []byte("message2")},
		{Key: []byte("key3"), Value: []byte("message3")},
	})

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
		RetryEnabled:       true,
		TransactionalRetry: kafka.NewBoolPtr(false),
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:       []string{"localhost:29092"},
			Topic:         "retry-topic",
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  20 * time.Second,
			MaxRetry:      3,
		},
		MessageGroupDuration: 5 * time.Second,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func batchConsumeFn(messages []*kafka.Message) error {
	// you can add custom error handling here & flag messages
	for i := range messages {
		if i < 2 {
			messages[i].IsFailed = true

			var retryCount string
			retryCountHeader := messages[i].Header("x-retry-count")
			if retryCountHeader != nil {
				retryCount = string(retryCountHeader.Value)
			}

			messages[i].ErrDescription = fmt.Sprintf("Key = %s error, retry count %s", string(messages[i].Key), retryCount)
		}
	}

	// you must return error here to retry only failed messages
	return errors.New("err")
}
