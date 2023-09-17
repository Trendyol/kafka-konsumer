package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx context.Context, message Message) error
	ProduceBatch(ctx context.Context, messages []Message) error
	Close() error
}

type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type producer struct {
	w Writer
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

func (c *producer) ProduceBatch(ctx context.Context, messages []Message) error {
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for i := range messages {
		convertedMessage := kafka.Message(messages[i])
		kafkaMessages = append(kafkaMessages, convertedMessage)
	}
	return c.w.WriteMessages(ctx, kafkaMessages...)
}

func (c *producer) Close() error {
	return c.w.Close()
}
