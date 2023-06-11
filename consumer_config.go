package kafka

import (
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"runtime"
	"time"

	"github.com/segmentio/kafka-go"
)

type ReaderConfig kafka.ReaderConfig

type BatchConsumeFn func([]Message) error

type ConsumeFn func(Message) error

type ConsumerConfig struct {
	Reader ReaderConfig

	Rack string
	SASL *SASLConfig
	TLS  *TLSConfig

	// Concurrency default is runtime.NumCPU()
	Concurrency int

	ConsumeFn ConsumeFn

	RetryEnabled       bool
	RetryConfiguration RetryConfiguration

	BatchConfiguration BatchConfiguration

	APIEnabled          bool
	APIConfiguration    APIConfiguration
	MetricConfiguration MetricConfiguration

	// LogLevel default is info
	LogLevel LogLevel
	Logger   LoggerInterface
}

func (cfg *ConsumerConfig) newCronsumerConfig() *kcronsumer.Config {
	cronsumerCfg := kcronsumer.Config{
		Brokers: cfg.RetryConfiguration.Brokers,
		Consumer: kcronsumer.ConsumerConfig{
			GroupID:           cfg.Reader.GroupID,
			Topic:             cfg.RetryConfiguration.Topic,
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
		LogLevel: "info",
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

type RetryConfiguration struct {
	MaxRetry      int
	StartTimeCron string
	WorkDuration  time.Duration
	Brokers       []string
	Topic         string
	SASL          *SASLConfig
	TLS           *TLSConfig
	Rack          string
}

type BatchConfiguration struct {
	MessageGroupLimit    int
	MessageGroupDuration time.Duration
	BatchConsumeFn       BatchConsumeFn
}

func (cfg *ConsumerConfig) newKafkaDialer() (*kafka.Dialer, error) {
	if cfg.SASL == nil && cfg.TLS == nil {
		return nil, nil
	}

	dialer := newDialer()

	if err := fillLayer(dialer, cfg.SASL, cfg.TLS); err != nil {
		return nil, err
	}

	return dialer.Dialer, nil
}

func (cfg *ConsumerConfig) newKafkaReader() (*kafka.Reader, error) {
	cfg.validate()

	dialer, err := cfg.newKafkaDialer()
	if err != nil {
		return nil, err
	}

	reader := kafka.ReaderConfig(cfg.Reader)
	reader.Dialer = dialer
	if cfg.Rack != "" {
		reader.GroupBalancers = []kafka.GroupBalancer{kafka.RackAffinityGroupBalancer{Rack: cfg.Rack}}
	}

	return kafka.NewReader(reader), nil
}

func (cfg *ConsumerConfig) validate() {
	if cfg.Concurrency == 0 {
		cfg.Concurrency = runtime.NumCPU()
	}
}
