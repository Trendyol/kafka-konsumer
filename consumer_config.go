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

	SASL *SASLConfig
	TLS  *TLSConfig

	// Concurrency default is runtime.NumCPU()
	Concurrency int

	ConsumeFn ConsumeFn

	RetryEnabled       bool
	RetryConfiguration RetryConfiguration

	// LogLevel default is info
	LogLevel LogLevel
	Logger   LoggerInterface
}

type RetryConfiguration struct {
	MaxRetry      int
	Topic         string
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

	return kafka.NewReader(reader), nil
}

func (c *ConsumerConfig) validate() {
	if c.Concurrency == 0 {
		c.Concurrency = runtime.NumCPU()
	}
}
