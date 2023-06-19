package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type WriterConfig struct {
	ErrorLogger            kafka.Logger
	Logger                 kafka.Logger
	Balancer               kafka.Balancer
	Completion             func(messages []kafka.Message, err error)
	Topic                  string
	Brokers                []string
	ReadTimeout            time.Duration
	BatchTimeout           time.Duration
	BatchBytes             int64
	WriteTimeout           time.Duration
	RequiredAcks           kafka.RequiredAcks
	BatchSize              int
	WriteBackoffMax        time.Duration
	WriteBackoffMin        time.Duration
	MaxAttempts            int
	Async                  bool
	Compression            kafka.Compression
	AllowAutoTopicCreation bool
}

type ProducerConfig struct {
	SASL     *SASLConfig
	TLS      *TLSConfig
	ClientId string
	Writer   WriterConfig
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
	transport := &Transport{
		Transport: &kafka.Transport{
			ClientID: c.ClientId,
		},
	}

	if err := fillLayer(transport, c.SASL, c.TLS); err != nil {
		return nil, err
	}

	return transport.Transport, nil
}
