package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Trendyol/kafka-konsumer"
)

type user struct {
	Name string
	ID   int
}

var messages = []user{
	{ID: 1, Name: "foo"},
	{ID: 2, Name: "bar"},
	{ID: 3, Name: "baz"},
	{ID: 4, Name: "qux"},
	{ID: 5, Name: "fred"},
}

func main() {
	// create new kafka producer
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	})
	defer producer.Close()

	go func() {
		// produce messages at 1 seconds interval
		i := 0
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			if i == len(messages) {
				break
			}
			message := messages[i]
			bytes, _ := json.Marshal(message)

			_ = producer.Produce(context.Background(), kafka.Message{
				Topic: "konsumer",
				Key:   []byte(strconv.Itoa(message.ID)),
				Value: bytes,
			})
			i++
		}
	}()

	consumerCfg := &kafka.ConsumerConfig{
		APIEnabled:  true,
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "konsumer",
			GroupID: "konsumer.group.test",
		},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:       []string{"localhost:29092"},
			Topic:         "konsumer-retry",
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  50 * time.Second,
			MaxRetry:      3,
		},
		ConsumeFn: func(message kafka.Message) error {
			// mocking some background task
			time.Sleep(1 * time.Second)

			fmt.Printf("Message from %s with value %s is consumed successfully\n", message.Topic, string(message.Value))
			return nil
		},
	}

	// create new kafka consumer
	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	// start consumer
	consumer.Consume()

	fmt.Println("Consumer started!")

	// wait for interrupt signal to gracefully shut down the consumer
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
