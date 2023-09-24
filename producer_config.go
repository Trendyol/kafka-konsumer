package kafka

import (
	"go.opentelemetry.io/otel"
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

type TransportConfig struct {
	DialTimeout    time.Duration
	IdleTimeout    time.Duration
	MetadataTTL    time.Duration
	MetadataTopics []string
}

type ProducerConfig struct {
	Transport                       *TransportConfig
	SASL                            *SASLConfig
	TLS                             *TLSConfig
	ClientID                        string
	Writer                          WriterConfig
	DistributedTracingEnabled       bool
	DistributedTracingConfiguration DistributedTracingConfiguration
}

func (cfg *ProducerConfig) newKafkaTransport() (*kafka.Transport, error) {
	transport := &Transport{
		Transport: &kafka.Transport{
			ClientID: cfg.ClientID,
		},
	}

	if cfg.Transport != nil {
		transport.Transport.DialTimeout = cfg.Transport.DialTimeout
		transport.Transport.IdleTimeout = cfg.Transport.IdleTimeout
		transport.Transport.MetadataTTL = cfg.Transport.MetadataTTL
		transport.Transport.MetadataTopics = cfg.Transport.MetadataTopics
	}

	if err := fillLayer(transport, cfg.SASL, cfg.TLS); err != nil {
		return nil, err
	}

	return transport.Transport, nil
}

func (cfg *ProducerConfig) setDefaults() {
	if cfg.DistributedTracingEnabled {
		if cfg.DistributedTracingConfiguration.TracerProvider == nil {
			cfg.DistributedTracingConfiguration.TracerProvider = otel.GetTracerProvider()
		}
		if cfg.DistributedTracingConfiguration.Propagator == nil {
			cfg.DistributedTracingConfiguration.Propagator = otel.GetTextMapPropagator()
		}
	}
}
