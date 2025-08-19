package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Trendyol/kafka-konsumer/v2"
)

const (
	topicName           = "standart-topic"
	batchTopicName      = "batch-topic"
	retryTopicName      = "retry-topic"
	deadLetterTopicName = "error-topic"
)

func main() {
	// Choose example type
	if len(os.Args) > 1 && os.Args[1] == "batch" {
		runBatchExample()
	} else {
		runSingleExample()
	}
}

func runSingleExample() {
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	})

	_ = producer.Produce(context.Background(), kafka.Message{
		Topic: topicName,
		Key:   []byte("1"),
		Value: []byte(`{ "foo": "bar" }`),
		Headers: []kafka.Header{
			{
				Key:   "x-elif",
				Value: []byte("11"),
			},
		},
	})

	consumerCfg := &kafka.ConsumerConfig{
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   topicName,
			GroupID: "konsumer.group.test",
		},
		DeadLetterTopic: deadLetterTopicName,
		ConsumeFn:       consumeFn,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Single Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func runBatchExample() {
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	})

	// Produce multiple messages for batch testing
	for i := 1; i <= 5; i++ {
		_ = producer.Produce(context.Background(), kafka.Message{
			Topic: batchTopicName,
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf(`{ "id": %d, "data": "test%d" }`, i, i)),
		})
	}

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   batchTopicName,
			GroupID: "batch.konsumer.group.test",
		},
		BatchConfiguration: &kafka.BatchConfiguration{
			BatchConsumeFn: batchConsumeFn,
		},
		TransactionalRetry: kafka.NewBoolPtr(false),
		RetryEnabled:       true,
		RetryConfiguration: kafka.RetryConfiguration{
			DeadLetterTopic: deadLetterTopicName,
			Brokers:         []string{"localhost:29092"},
			Topic:           retryTopicName,
			StartTimeCron:   "*/1 * * * *",
			WorkDuration:    50 * time.Second,
			MaxRetry:        1,
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Batch Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func consumeFn(message *kafka.Message) error {
	fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))
	message.SendDirectToDeadLetter = true
	// returns error to be sent to dead-letter topic
	return errors.New("consumer error")
}

func batchConsumeFn(messages []*kafka.Message) error {
	fmt.Printf("Processing batch of %d messages\n", len(messages))

	// Mark some messages to be sent directly to dead letter
	for i, message := range messages {
		fmt.Printf("Message %d: Key=%s, Value=%s\n", i+1, string(message.Key), string(message.Value))

		// Send messages with even keys directly to dead letter
		if string(message.Key) == "2" || string(message.Key) == "4" {
			message.SendDirectToDeadLetter = true
			message.ErrDescription = string(message.Key) + " error"
			fmt.Printf("  -> Marked for direct dead letter: %s\n", string(message.Key))
		}
	}

	// Return error to trigger the SendDirectToDeadLetter logic
	return errors.New("batch processing error")
}
