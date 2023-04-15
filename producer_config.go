package kafka

import (
	"github.com/segmentio/kafka-go"
	"time"
)

type WriterConfig struct {
	Topic                  string
	Brokers                []string
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

func (c ProducerConfig) newKafkaWriter() (*kafka.Writer, error) {
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(c.Writer.Brokers...),
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
		AllowAutoTopicCreation: c.Writer.AllowAutoTopicCreation,
	}

	if c.SASL != nil || c.TLS != nil {
		transport, err := c.newKafkaTransport()
		if err != nil {
			return nil, err
		}
		kafkaWriter.Transport = transport
	}

	return kafkaWriter, nil
}

func (c ProducerConfig) newKafkaTransport() (*kafka.Transport, error) {
	transport := newTransport()
	if err := fillLayer(transport, c.SASL, c.TLS); err != nil {
		return nil, err
	}

	return transport.Transport, nil
}
