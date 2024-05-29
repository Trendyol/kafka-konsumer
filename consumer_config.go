package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	lcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

type ReaderConfig kafka.ReaderConfig

type BatchConsumeFn func([]*Message) error

type PreBatchFn func([]*Message) []*Message

type ConsumeFn func(*Message) error

type SkipMessageByHeaderFn func(header []kafka.Header) bool

type DialConfig struct {
	Timeout   time.Duration
	KeepAlive time.Duration
}

type ConsumerConfig struct {
	DistributedTracingConfiguration DistributedTracingConfiguration
	Logger                          LoggerInterface
	APIConfiguration                APIConfiguration
	MetricConfiguration             MetricConfiguration
	SASL                            *SASLConfig
	TLS                             *TLSConfig
	Dial                            *DialConfig
	BatchConfiguration              *BatchConfiguration
	ConsumeFn                       ConsumeFn
	SkipMessageByHeaderFn           SkipMessageByHeaderFn
	TransactionalRetry              *bool
	RetryConfiguration              RetryConfiguration
	LogLevel                        LogLevel
	Rack                            string
	ClientID                        string
	Reader                          ReaderConfig
	CommitInterval                  time.Duration
	MessageGroupDuration            time.Duration
	Concurrency                     int
	DistributedTracingEnabled       bool
	RetryEnabled                    bool
	APIEnabled                      bool

	// MetricPrefix is used for prometheus fq name prefix.
	// If not provided, default metric prefix value is `kafka_konsumer`.
	// Currently, there are two exposed prometheus metrics. `processed_messages_total_current` and `unprocessed_messages_total_current`.
	// So, if default metric prefix used, metrics names are `kafka_konsumer_processed_messages_total_current` and
	// `kafka_konsumer_unprocessed_messages_total_current`.
	MetricPrefix string
}

func (cfg *ConsumerConfig) newCronsumerConfig() *kcronsumer.Config {
	cronsumerCfg := kcronsumer.Config{
		MetricPrefix: cfg.RetryConfiguration.MetricPrefix,
		ClientID:     cfg.RetryConfiguration.ClientID,
		Brokers:      cfg.RetryConfiguration.Brokers,
		Consumer: kcronsumer.ConsumerConfig{
			ClientID:          cfg.ClientID,
			GroupID:           cfg.Reader.GroupID,
			Topic:             cfg.RetryConfiguration.Topic,
			DeadLetterTopic:   cfg.RetryConfiguration.DeadLetterTopic,
			Cron:              cfg.RetryConfiguration.StartTimeCron,
			Duration:          cfg.RetryConfiguration.WorkDuration,
			Concurrency:       cfg.Concurrency,
			MinBytes:          cfg.Reader.MinBytes,
			MaxBytes:          cfg.Reader.MaxBytes,
			MaxRetry:          cfg.RetryConfiguration.MaxRetry,
			MaxWait:           cfg.Reader.MaxWait,
			CommitInterval:    cfg.Reader.CommitInterval,
			HeartbeatInterval: cfg.Reader.HeartbeatInterval,
			SessionTimeout:    cfg.Reader.SessionTimeout,
			RebalanceTimeout:  cfg.Reader.RebalanceTimeout,
			StartOffset:       kcronsumer.ToStringOffset(cfg.Reader.StartOffset),
			RetentionTime:     cfg.Reader.RetentionTime,
		},
		Producer: kcronsumer.ProducerConfig{
			Balancer: cfg.RetryConfiguration.Balancer,
			Brokers:  cfg.RetryConfiguration.Brokers,
		},
		LogLevel: lcronsumer.Level(cfg.RetryConfiguration.LogLevel),
	}

	if cfg.RetryConfiguration.SkipMessageByHeaderFn != nil {
		cronsumerCfg.Consumer.SkipMessageByHeaderFn = func(headers []kcronsumer.Header) bool {
			return cfg.RetryConfiguration.SkipMessageByHeaderFn(toHeaders(headers))
		}
	}

	if !cfg.RetryConfiguration.SASL.IsEmpty() {
		cronsumerCfg.SASL.Enabled = true
		cronsumerCfg.SASL.AuthType = string(cfg.RetryConfiguration.SASL.Type)
		cronsumerCfg.SASL.Username = cfg.RetryConfiguration.SASL.Username
		cronsumerCfg.SASL.Password = cfg.RetryConfiguration.SASL.Password
		cronsumerCfg.SASL.Rack = cfg.RetryConfiguration.Rack
	}

	if !cfg.RetryConfiguration.TLS.IsEmpty() {
		cronsumerCfg.SASL.RootCAPath = cfg.RetryConfiguration.TLS.RootCAPath
		cronsumerCfg.SASL.IntermediateCAPath = cfg.RetryConfiguration.TLS.IntermediateCAPath
	}

	return &cronsumerCfg
}

type APIConfiguration struct {
	// Port default is 8090
	Port *int

	// HealthCheckPath default is /healthcheck
	HealthCheckPath *string
}

type MetricConfiguration struct {
	// Path default is /metrics
	Path *string
}

type DistributedTracingConfiguration struct {
	TracerProvider trace.TracerProvider
	Propagator     propagation.TextMapPropagator
}

func toHeaders(cronsumerHeaders []kcronsumer.Header) []Header {
	headers := make([]Header, 0, len(cronsumerHeaders))
	for i := range cronsumerHeaders {
		headers = append(headers, Header{
			Key:   cronsumerHeaders[i].Key,
			Value: cronsumerHeaders[i].Value,
		})
	}
	return headers
}

type RetryConfiguration struct {
	// MetricPrefix is used for prometheus fq name prefix.
	// If not provided, default metric prefix value is `kafka_cronsumer`.
	// Currently, there are two exposed prometheus metrics. `retried_messages_total_current` and `discarded_messages_total_current`.
	// So, if default metric prefix used, metrics names are `kafka_cronsumer_retried_messages_total_current` and
	// `kafka_cronsumer_discarded_messages_total_current`.
	MetricPrefix string

	SASL                  *SASLConfig
	TLS                   *TLSConfig
	ClientID              string
	StartTimeCron         string
	Topic                 string
	DeadLetterTopic       string
	Rack                  string
	LogLevel              LogLevel
	Brokers               []string
	Balancer              Balancer
	MaxRetry              int
	WorkDuration          time.Duration
	SkipMessageByHeaderFn SkipMessageByHeaderFn
}

type BatchConfiguration struct {
	BatchConsumeFn    BatchConsumeFn
	PreBatchFn        PreBatchFn
	MessageGroupLimit int
}

func (cfg *ConsumerConfig) newKafkaDialer() (*kafka.Dialer, error) {
	dialer := &Dialer{
		Dialer: &kafka.Dialer{
			ClientID: cfg.ClientID,
		},
	}

	if cfg.Dial != nil {
		dialer.Dialer.Timeout = cfg.Dial.Timeout
		dialer.Dialer.KeepAlive = cfg.Dial.KeepAlive
	}

	if cfg.SASL == nil && cfg.TLS == nil {
		return dialer.Dialer, nil
	}

	if err := fillLayer(dialer, cfg.SASL, cfg.TLS); err != nil {
		return nil, err
	}

	return dialer.Dialer, nil
}

func (cfg *ConsumerConfig) newKafkaReader() (Reader, error) {
	cfg.setDefaults()

	dialer, err := cfg.newKafkaDialer()
	if err != nil {
		return nil, err
	}

	readerCfg := kafka.ReaderConfig(cfg.Reader)
	readerCfg.Dialer = dialer
	if cfg.Rack != "" {
		readerCfg.GroupBalancers = []kafka.GroupBalancer{kafka.RackAffinityGroupBalancer{Rack: cfg.Rack}}
	}

	reader := kafka.NewReader(readerCfg)

	if cfg.DistributedTracingEnabled {
		return NewOtelReaderWrapper(cfg, reader)
	}

	return NewReaderWrapper(reader), nil
}

func (cfg *ConsumerConfig) setDefaults() {
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}

	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = time.Second
		// Kafka-go library default value is 0, we need to also change this.
		cfg.Reader.CommitInterval = time.Second
	} else {
		cfg.Reader.CommitInterval = cfg.CommitInterval
	}

	if cfg.MessageGroupDuration == 0 {
		cfg.MessageGroupDuration = time.Second
	}

	if cfg.DistributedTracingEnabled {
		if cfg.DistributedTracingConfiguration.Propagator == nil {
			cfg.DistributedTracingConfiguration.Propagator = otel.GetTextMapPropagator()
		}
		if cfg.DistributedTracingConfiguration.TracerProvider == nil {
			cfg.DistributedTracingConfiguration.TracerProvider = otel.GetTracerProvider()
		}
	}
	if cfg.TransactionalRetry == nil {
		cfg.TransactionalRetry = NewBoolPtr(true)
	}
}

func NewBoolPtr(value bool) *bool {
	return &value
}
