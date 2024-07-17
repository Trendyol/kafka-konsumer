package main

import (
	"encoding/json"
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
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
			MessageGroupLimit: 1000,
			BatchConsumeFn:    batchConsumeFn,
			PreBatchFn:        preBatch,
		},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:       []string{"localhost:29092"},
			Topic:         "retry-topic",
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  50 * time.Second,
			MaxRetry:      3,
		},
		MessageGroupDuration: time.Second,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

type MessageValue struct {
	Id           int            `json:"id"`
	Version      int            `json:"version"`
	Payload      string         `json:"payload"`
	KafkaMessage *kafka.Message `json:"-"`
}

func preBatch(messages []*kafka.Message) []*kafka.Message {
	latestMessageById := getLatestMessageByID(messages)

	return convertLatestMessageList(latestMessageById)
}

func getLatestMessageByID(messages []*kafka.Message) map[int]MessageValue {
	latestMessageById := make(map[int]MessageValue, len(messages))
	for i := range messages {
		var mv MessageValue
		json.Unmarshal(messages[i].Value, &mv)
		mv.KafkaMessage = messages[i]

		val, ok := latestMessageById[mv.Id]
		if !ok {
			latestMessageById[mv.Id] = mv
		} else if mv.Version > val.Version {
			latestMessageById[mv.Id] = mv
		}
	}
	return latestMessageById
}

func convertLatestMessageList(latestMessageById map[int]MessageValue) []*kafka.Message {
	result := make([]*kafka.Message, 0, len(latestMessageById))
	for _, latestMessage := range latestMessageById {
		result = append(result, latestMessage.KafkaMessage)
	}
	return result
}

// In order to load topic with data, use:
// kafka-console-producer --broker-list localhost:29092 --topic standart-topic < examples/with-kafka-batch-consumer-with-prebatch/testdata/messages.txt
func batchConsumeFn(messages []*kafka.Message) error {
	fmt.Printf("length of %d messages, first message value is %s \n", len(messages), messages[0].Value)

	return nil
}
