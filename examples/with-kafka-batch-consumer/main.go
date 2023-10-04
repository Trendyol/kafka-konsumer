package main

import (
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
		ManuelRetryEnabled: true,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	_ = consumer.WithRetryFunc()
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

// In order to load topic with data, use:
// kafka-console-producer --broker-list localhost:29092 --topic standart-topic < examples/with-kafka-batch-consumer/load.txt
func batchConsumeFn(messages []kafka.Message) error {
	fmt.Printf("%d\n comes first %s", len(messages), messages[0].Value)
	return nil
}
