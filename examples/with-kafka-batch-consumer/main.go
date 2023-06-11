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
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "standart-topic",
			GroupID: "standart-cg",
		},
		LogLevel: kafka.LogLevelDebug,
		BatchConfiguration: kafka.BatchConfiguration{
			MessageGroupLimit:    1000,
			MessageGroupDuration: time.Second,
			BatchConsumeFn:       batchConsumeFn,
		},
	}

	consumer, _ := kafka.NewBatchConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

// kafka-console-producer --broker-list localhost:29092 --topic standart-topic < examples/with-kafka-batch-consumer/load.txt
func batchConsumeFn(messages []kafka.Message) error {
	fmt.Printf("%d\n comes first %s", len(messages), messages[0].Value)
	return nil
}
