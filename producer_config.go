package kafka

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel"

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

func (cfg WriterConfig) Json() string {
	return fmt.Sprintf(`{"Brokers": ["%s"], "Balancer": %q, "Compression": %q}`,
		strings.Join(cfg.Brokers, "\", \""), GetBalancerString(cfg.Balancer), cfg.Compression.String())
}

type TransportConfig struct {
	MetadataTopics []string
	DialTimeout    time.Duration
	IdleTimeout    time.Duration
	MetadataTTL    time.Duration
}

type ProducerConfig struct {
	DistributedTracingConfiguration DistributedTracingConfiguration
	Transport                       *TransportConfig
	SASL                            *SASLConfig
	TLS                             *TLSConfig
	ClientID                        string
	Writer                          WriterConfig
	DistributedTracingEnabled       bool
}

func (cfg *ProducerConfig) String() string {
	re := regexp.MustCompile(`"(\w+)"\s*:`)
	modifiedString := re.ReplaceAllString(cfg.JSON(), `$1:`)
	modifiedString = modifiedString[1 : len(modifiedString)-1]
	return modifiedString
}

func (cfg *ProducerConfig) JSON() string {
	if cfg == nil {
		return "{}"
	}
	return fmt.Sprintf(`{"Writer": %s, "ClientID": %q, "DistributedTracingEnabled": %t, "SASL": %s, "TLS": %s}`,
		cfg.Writer.Json(), cfg.ClientID, cfg.DistributedTracingEnabled, cfg.SASL.JSON(), cfg.TLS.JSON())
}

func (cfg *ProducerConfig) JSONPretty() string {
	return jsonPretty(cfg.JSON())
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
