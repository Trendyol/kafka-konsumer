package main

import (
	"fmt"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-konsumer"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM, syscall.SIGINT)

	consumerCfg := kafka.ConsumerConfig{
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "standart-topic",
			GroupID: "standart-cg",
		},
		CronsumerEnabled: true,
		CronsumerConfig: &kcronsumer.Config{
			Brokers: []string{"localhost:29092"},
			Consumer: kcronsumer.ConsumerConfig{
				GroupID:     "standart-cg",
				Topic:       "exception-topic",
				Concurrency: 1,
				Cron:        "*/1 * * * *",
				Duration:    50 * time.Second,
			},
			LogLevel: "info",
		},
		ExceptionFunc: func(message kcronsumer.Message) error {
			fmt.Println(message)
			return nil
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)

	consumer.Consume(func(message kafka.Message) {
		fmt.Println(message.Value)
	})

	defer consumer.Stop()

	fmt.Println("Consumer started...!")

	<-gracefulShutdown
}
