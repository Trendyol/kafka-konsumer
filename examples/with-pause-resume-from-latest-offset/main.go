package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	kafka "github.com/Trendyol/kafka-konsumer/v2"
)

func main() {
	consumerCfg := &kafka.ConsumerConfig{
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   "standart-topic",
			GroupID: "resume-from-latest-cg",
		},
		RetryEnabled: false,
		ConsumeFn:    consumeFn,
	}

	consumer, err := kafka.NewConsumer(consumerCfg)
	if err != nil {
		fmt.Printf("Failed to create consumer: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Stop()

	consumer.Consume()
	fmt.Println("Consumer started...!")

	// Simulate pause-resume-from-latest-offset workflow
	go func() {
		// Let consumer run for 10 seconds
		fmt.Println("Consumer is consuming messages...")
		time.Sleep(2 * time.Second)

		// Pause the consumer
		consumer.Pause()
		fmt.Println("\n=== Consumer PAUSED ===")
		fmt.Println("Messages produced during this pause will be skipped...")

		// Wait for 15 seconds while consumer is paused
		// During this time, any messages produced to the topic will be skipped
		time.Sleep(15 * time.Second)

		// Resume from latest offset (skip messages that arrived during pause)
		fmt.Println("\n=== Resuming from LATEST OFFSET ===")
		fmt.Println("Skipping messages that arrived during pause period...")

		err = consumer.ResumeFromLatestOffset()
		if err != nil {
			fmt.Printf("Failed to resume from latest offset: %v\n", err)
			return
		}

		fmt.Println("Consumer resumed from latest offset!")
		fmt.Println("Now consuming only new messages...")

		// Let it run for another 10 seconds
		time.Sleep(10 * time.Second)

		// Pause again to demonstrate idempotency
		consumer.Pause()
		fmt.Println("\n=== Consumer PAUSED again ===")

		time.Sleep(10 * time.Second)

		// Regular resume (will NOT skip messages)
		fmt.Println("\n=== Regular RESUME (will process all pending messages) ===")
		consumer.Resume()
		fmt.Println("Consumer resumed normally!")
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func consumeFn(message *kafka.Message) error {
	fmt.Printf("[%s] Consumed message: Partition=%d, Offset=%d, Value=%s\n",
		time.Now().Format("15:04:05"),
		message.Partition,
		message.Offset,
		string(message.Value))
	return nil
}
