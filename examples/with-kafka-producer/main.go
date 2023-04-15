package main

import (
	"context"
	"github.com/Trendyol/kafka-konsumer"
)

func main() {
	producer, _ := kafka.NewProducer(kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
	})

	_ = producer.Produce(context.Background(), kafka.Message{
		Topic: "standart-topic",
		Key:   []byte("1"),
		Value: []byte(`{ "foo": "bar" }`),
	})
}
