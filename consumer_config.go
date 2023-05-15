package kafka

import (
	"runtime"
	"time"

	"github.com/segmentio/kafka-go"
)

type ReaderConfig kafka.ReaderConfig

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

	APIEnabled          bool
	APIConfiguration    APIConfiguration
	MetricConfiguration MetricConfiguration

	// LogLevel default is info
	LogLevel LogLevel
	Logger   LoggerInterface
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
	Brokers       []string
	Topic         string
	MaxRetry      int
	StartTimeCron string
	WorkDuration  time.Duration
}

func (c *ConsumerConfig) newKafkaDialer() (*kafka.Dialer, error) {
	if c.SASL == nil && c.TLS == nil {
		return nil, nil
	}

	dialer := newDialer()

	if err := fillLayer(dialer, c.SASL, c.TLS); err != nil {
		return nil, err
	}

	return dialer.Dialer, nil
}

func (c *ConsumerConfig) newKafkaReader() (*kafka.Reader, error) {
	c.validate()

	dialer, err := c.newKafkaDialer()
	if err != nil {
		return nil, err
	}

	reader := kafka.ReaderConfig(c.Reader)
	reader.Dialer = dialer
	if c.Rack != "" {
		reader.GroupBalancers = []kafka.GroupBalancer{kafka.RackAffinityGroupBalancer{Rack: c.Rack}}
	}

	return kafka.NewReader(reader), nil
}

func (c *ConsumerConfig) validate() {
	if c.Concurrency == 0 {
		c.Concurrency = runtime.NumCPU()
	}
}
