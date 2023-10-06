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

func NewProducer(cfg *ProducerConfig) (Producer, error) {
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Writer.Brokers...),
		Topic:                  cfg.Writer.Topic,
		Balancer:               cfg.Writer.Balancer,
		MaxAttempts:            cfg.Writer.MaxAttempts,
		WriteBackoffMin:        cfg.Writer.WriteBackoffMin,
		WriteBackoffMax:        cfg.Writer.WriteBackoffMax,
		BatchSize:              cfg.Writer.BatchSize,
		BatchBytes:             cfg.Writer.BatchBytes,
		BatchTimeout:           cfg.Writer.BatchTimeout,
		ReadTimeout:            cfg.Writer.ReadTimeout,
		WriteTimeout:           cfg.Writer.WriteTimeout,
		RequiredAcks:           cfg.Writer.RequiredAcks,
		Async:                  cfg.Writer.Async,
		Completion:             cfg.Writer.Completion,
		Compression:            cfg.Writer.Compression,
		Logger:                 cfg.Writer.Logger,
		ErrorLogger:            cfg.Writer.ErrorLogger,
		AllowAutoTopicCreation: cfg.Writer.AllowAutoTopicCreation,
	}

	if cfg.SASL != nil || cfg.TLS != nil {
		transport, err := cfg.newKafkaTransport()
		if err != nil {
			return nil, err
		}
		kafkaWriter.Transport = transport
	}

	p := &producer{w: kafkaWriter}

	if cfg.DistributedTracingEnabled {
		otelWriter, err := NewOtelProducer(cfg, kafkaWriter)
		if err != nil {
			return nil, err
		}
		p.w = otelWriter
	}

	return p, nil
}

func (c *producer) Produce(ctx context.Context, message Message) error {
	return c.w.WriteMessages(ctx, message.toKafkaMessage())
}

func (c *producer) ProduceBatch(ctx context.Context, messages []Message) error {
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for i := range messages {
		kafkaMessages = append(kafkaMessages, messages[i].toKafkaMessage())
	}
	return c.w.WriteMessages(ctx, kafkaMessages...)
}

func (c *producer) Close() error {
	return c.w.Close()
}
