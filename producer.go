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
	w            Writer
	interceptors []ProducerInterceptor
}

func NewProducer(cfg *ProducerConfig, interceptors ...ProducerInterceptor) (Producer, error) {
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

	p := &producer{w: kafkaWriter, interceptors: interceptors}

	if cfg.DistributedTracingEnabled {
		otelWriter, err := NewOtelProducer(cfg, kafkaWriter)
		if err != nil {
			return nil, err
		}
		p.w = otelWriter
	}

	return p, nil
}

func (p *producer) Produce(ctx context.Context, message Message) error {
	if len(p.interceptors) > 0 {
		p.executeInterceptors(ctx, &message)
	}

	return p.w.WriteMessages(ctx, message.toKafkaMessage())
}

func (p *producer) ProduceBatch(ctx context.Context, messages []Message) error {
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for i := range messages {
		if len(p.interceptors) > 0 {
			p.executeInterceptors(ctx, &messages[i])
		}

		kafkaMessages = append(kafkaMessages, messages[i].toKafkaMessage())
	}

	return p.w.WriteMessages(ctx, kafkaMessages...)
}

func (p *producer) executeInterceptors(ctx context.Context, message *Message) {
	for _, interceptor := range p.interceptors {
		interceptor.OnProduce(ProducerInterceptorContext{Context: ctx, Message: message})
	}
}

func (p *producer) Close() error {
	return p.w.Close()
}
