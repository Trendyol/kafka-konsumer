package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	"strconv"
	"sync/atomic"
	"time"
)

type user struct {
	ID   int
	Name string
}

var messages = []user{
	{ID: 1, Name: "foo"},
	{ID: 2, Name: "bar"},
	{ID: 3, Name: "baz"},
	{ID: 4, Name: "qux"},
	{ID: 5, Name: "fred"},
}

func main() {
	// retryMap stores the number of retries for each message
	var retryMap = make(map[int]int, len(messages))
	for _, message := range messages {
		retryMap[message.ID] = 0
	}

	// create new kafka producer
	producer, _ := kafka.NewProducer(kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	})

	// produce messages at 1 seconds interval
	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})
	go func() {
		var i uint64
		for {
			select {
			case <-ticker.C:
				message := messages[atomic.LoadUint64(&i)]
				bytes, _ := json.Marshal(message)

				_ = producer.Produce(context.Background(), kafka.Message{
					Topic: "konsumer",
					Key:   []byte(strconv.Itoa(message.ID)),
					Value: bytes,
				})

				if message.ID == messages[len(messages)-1].ID {
					quit <- struct{}{}
					return
				}

				atomic.AddUint64(&i, 1)
			case <-quit:
				ticker.Stop()
				return
			}
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
			u := &user{}
			if err := json.Unmarshal(message.Value, u); err != nil {
				return err
			}

			n := retryMap[u.ID]
			if n < 3 {
				retryMap[u.ID] += 1
				return fmt.Errorf("message %s retrying, current retry count: %d", message.Key, n)
			}

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
	select {}
}
