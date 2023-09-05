package kafka

import (
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	lcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/logger"

	"github.com/segmentio/kafka-go"
)

type ReaderConfig kafka.ReaderConfig

type BatchConsumeFn func([]Message) error

type ConsumeFn func(Message) error

type DialConfig struct {
	Timeout   time.Duration
	KeepAlive time.Duration
}

type ConsumerConfig struct {
	APIConfiguration    APIConfiguration
	Logger              LoggerInterface
	MetricConfiguration MetricConfiguration
	SASL                *SASLConfig
	TLS                 *TLSConfig
	Dial                *DialConfig
	BatchConfiguration  *BatchConfiguration
	ConsumeFn           ConsumeFn
	ClientID            string
	Rack                string
	LogLevel            LogLevel
	Reader              ReaderConfig
	RetryConfiguration  RetryConfiguration
	CommitInterval      time.Duration
	Concurrency         int
	RetryEnabled        bool
	APIEnabled          bool
}

func (cfg *ConsumerConfig) newCronsumerConfig() *kcronsumer.Config {
	cronsumerCfg := kcronsumer.Config{
		ClientID: cfg.RetryConfiguration.ClientID,
		Brokers:  cfg.RetryConfiguration.Brokers,
		Consumer: kcronsumer.ConsumerConfig{
			ClientID:          cfg.ClientID,
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
		LogLevel: lcronsumer.Level(cfg.RetryConfiguration.LogLevel),
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
	SASL          *SASLConfig
	TLS           *TLSConfig
	ClientID      string
	StartTimeCron string
	Topic         string
	Rack          string
	Brokers       []string
	MaxRetry      int
	WorkDuration  time.Duration
	LogLevel      LogLevel
}

type BatchConfiguration struct {
	BatchConsumeFn       BatchConsumeFn
	MessageGroupLimit    int
	MessageGroupDuration time.Duration
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
		cfg.Concurrency = 1
	}

	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = time.Second
		// Kafka-go library default value is 0, we need to also change this.
		cfg.Reader.CommitInterval = time.Second
	} else {
		cfg.Reader.CommitInterval = cfg.CommitInterval
	}
}
