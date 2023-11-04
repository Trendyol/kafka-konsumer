package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Trendyol/kafka-konsumer"
)

func main() {
	consumerCfg := &kafka.ConsumerConfig{
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29093"},
			Topic:   "standard-topic",
			GroupID: "standard-cg",
		},
		RetryEnabled: false,
		ConsumeFn:    consumeFn,
		SASL: &kafka.SASLConfig{
			Type:     kafka.MechanismPlain,
			Username: "client",
			Password: "client-secret",
		},
		TLS:  nil,
		Rack: "rack1",
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
