package kafka

import (
	"github.com/segmentio/kafka-go"
	"runtime"
)

type ReaderConfig kafka.ReaderConfig

type ConsumerConfig struct {
	SASL *SASLConfig
	TLS  *CertLoader

	// Concurrency default is runtime.NumCPU()
	Concurrency int
	Reader      ReaderConfig
}

func (c ConsumerConfig) newKafkaDialer() (*kafka.Dialer, error) {
	if c.SASL == nil && c.TLS == nil {
		return nil, nil
	}

	dialer := newDialer()
	if err := fillLayer(dialer, c.SASL, c.TLS); err != nil {
		return nil, err
	}

	return dialer.Dialer, nil
}

func (c ConsumerConfig) newKafkaReader() (*kafka.Reader, error) {
	c.validate()

	dialer, err := c.newKafkaDialer()
	if err != nil {
		return nil, err
	}

	reader := kafka.ReaderConfig(c.Reader)
	reader.Dialer = dialer

	return kafka.NewReader(reader), nil
}

func (c ConsumerConfig) validate() {
	if c.Concurrency == 0 {
		c.Concurrency = runtime.NumCPU()
	}
}
