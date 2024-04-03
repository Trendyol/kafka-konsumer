package main

import (
	"fmt"
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
		RetryEnabled: false,
		ConsumeFn:    consumeFn,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()
	fmt.Println("Consumer started...!")

	// You can produce a message via kowl.
	go func() {
		time.Sleep(10 * time.Second)
		consumer.Pause()

		time.Sleep(10 * time.Second)
		consumer.Resume()

		time.Sleep(10 * time.Second)
		consumer.Pause()

		time.Sleep(10 * time.Second)
		consumer.Resume()

		time.Sleep(10 * time.Second)
		consumer.Pause()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func consumeFn(message *kafka.Message) error {
	fmt.Printf("Message From %s with value %s \n", message.Topic, string(message.Value))
	return nil
}
