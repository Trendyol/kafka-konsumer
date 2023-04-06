package main

import (
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	"time"
)

var counter = 0

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
			Topic:         "retry-topic",
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  50 * time.Second,
			MaxRetry:      3,
		},
		ConsumeFn: consumeFn,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)

	defer consumer.Stop()
	consumer.Consume()

	fmt.Println("Consumer started...!")
	select {}
}

func consumeFn(message kafka.Message) error {
	fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))

	time.Sleep(time.Second)
	counter++

	if counter == 1 || counter == 2 {
		fmt.Println("Sending " + string(message.Value) + "to retry topic!")
		return errors.New("some error")
	}

	return nil
}
