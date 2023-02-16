package kafka

import (
	"github.com/segmentio/kafka-go"
	"time"
)

type WriterConfig struct {
	Topic                  string
	Servers                []string
	MaxAttempts            int
	WriteBackoffMin        time.Duration
	WriteBackoffMax        time.Duration
	BatchSize              int
	BatchBytes             int64
	BatchTimeout           time.Duration
	ReadTimeout            time.Duration
	WriteTimeout           time.Duration
	RequiredAcks           kafka.RequiredAcks
	Async                  bool
	Compression            kafka.Compression
	AllowAutoTopicCreation bool
	Balancer               kafka.Balancer
	Logger                 kafka.Logger
	ErrorLogger            kafka.Logger
	Completion             func(messages []kafka.Message, err error)
}

type ProducerConfig struct {
	Writer WriterConfig
	SASL   *SASLConfig
	TLS    *CertLoader
}

func (c ProducerConfig) newKafkaTransport() (*kafka.Transport, error) {
	if c.SASL == nil && c.TLS == nil {
		return nil, nil
	}

	transport := newTransport()
	if err := fillLayer(transport, c.SASL, c.TLS); err != nil {
		return nil, err
	}

	return transport.Transport, nil
}

func (c ProducerConfig) newKafkaWriter() (*kafka.Writer, error) {
	transport, err := c.newKafkaTransport()
	if err != nil {
		return nil, err
	}

	return &kafka.Writer{
		Addr:                   kafka.TCP(c.Writer.Servers...),
		Topic:                  c.Writer.Topic,
		Balancer:               c.Writer.Balancer,
		MaxAttempts:            c.Writer.MaxAttempts,
		WriteBackoffMin:        c.Writer.WriteBackoffMin,
		WriteBackoffMax:        c.Writer.WriteBackoffMax,
		BatchSize:              c.Writer.BatchSize,
		BatchBytes:             c.Writer.BatchBytes,
		BatchTimeout:           c.Writer.BatchTimeout,
		ReadTimeout:            c.Writer.ReadTimeout,
		WriteTimeout:           c.Writer.WriteTimeout,
		RequiredAcks:           c.Writer.RequiredAcks,
		Async:                  c.Writer.Async,
		Completion:             c.Writer.Completion,
		Compression:            c.Writer.Compression,
		Logger:                 c.Writer.Logger,
		ErrorLogger:            c.Writer.ErrorLogger,
		Transport:              transport,
		AllowAutoTopicCreation: c.Writer.AllowAutoTopicCreation,
	}, nil
}
