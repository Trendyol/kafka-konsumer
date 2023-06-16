package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx context.Context, message Message) error
	Close() error
}

type producer struct {
	w *kafka.Writer
}

func NewProducer(cfg ProducerConfig) (Producer, error) {
	writer, err := cfg.newKafkaWriter()
	if err != nil {
		return nil, err
	}

	return &producer{w: writer}, nil
}

func (c *producer) Produce(ctx context.Context, message Message) error {
	return c.w.WriteMessages(ctx, kafka.Message(message))
}

func (c *producer) Close() error {
	return c.w.Close()
}
