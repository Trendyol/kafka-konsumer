package main

import (
	"context"
	"github.com/Trendyol/kafka-konsumer/v2"
)

func main() {
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	}, nil)

	const topicName = "standart-topic"

	_ = producer.Produce(context.Background(), kafka.Message{
		Topic: topicName,
		Key:   []byte("1"),
		Value: []byte(`{ "foo": "bar" }`),
	})

	_ = producer.ProduceBatch(context.Background(), []kafka.Message{
		{
			Topic: topicName,
			Key:   []byte("1"),
			Value: []byte(`{ "foo": "bar" }`),
		},
		{
			Topic: topicName,
			Key:   []byte("2"),
			Value: []byte(`{ "foo2": "bar2" }`),
		},
	})
}
