package kafka

import (
	"context"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"sync"
)

type Consumer interface {
	Consume()
	Stop() error
}

type consumer struct {
	r           *kafka.Reader
	m           sync.Mutex
	wg          sync.WaitGroup
	once        sync.Once
	messageCh   chan Message
	quit        chan struct{}
	concurrency int
	cronsumer   kcronsumer.Cronsumer

	consumeFn func(Message) error

	retryEnabled bool
	retryFn      func(message kcronsumer.Message) error
	retryTopic   string
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	c := consumer{
		messageCh:    make(chan Message, cfg.Concurrency),
		quit:         make(chan struct{}),
		concurrency:  cfg.Concurrency,
		consumeFn:    cfg.ConsumeFn,
		retryEnabled: cfg.RetryEnabled,
	}

	var err error

	if c.r, err = cfg.newKafkaReader(); err != nil {
		return nil, err
	}

	if cfg.RetryEnabled {
		kcronsumerCfg := kcronsumer.Config{
			Brokers: cfg.Reader.Brokers,
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

		if !cfg.SASL.IsEmpty() {
			kcronsumerCfg.SASL.AuthType = string(cfg.SASL.Type)
			kcronsumerCfg.SASL.Username = cfg.SASL.Username
			kcronsumerCfg.SASL.Password = cfg.SASL.Password
		}

		if !cfg.TLS.IsEmpty() {
			kcronsumerCfg.SASL.RootCAPath = cfg.TLS.RootCAPath
			kcronsumerCfg.SASL.IntermediateCAPath = cfg.TLS.IntermediateCAPath
		}

		c.retryTopic = cfg.RetryConfiguration.Topic

		c.retryFn = func(message kcronsumer.Message) error {
			return c.consumeFn(convertFromRetryableMessage(message))
		}

		c.cronsumer = cronsumer.New(&kcronsumerCfg, c.retryFn)
	}

	return &c, nil
}

func (c *consumer) Consume() {
	if c.retryEnabled {
		c.cronsumer.Start()
	}

	go c.consume()

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for message := range c.messageCh {
				if err := c.consumeFn(message); err == nil {
					continue
				}

				if c.retryEnabled {
					retryableMsg := convertToRetryableMessage(c.retryTopic, message)
					if err := c.retryFn(retryableMsg); err != nil {
						_ = c.cronsumer.Produce(retryableMsg)
					}
				}
			}
		}()
	}
}

func convertToRetryableMessage(retryTopic string, message Message) kcronsumer.Message {
	headers := make([]kcronsumer.Header, 0, len(message.Headers))
	for i := range message.Headers {
		headers = append(headers, kcronsumer.Header{
			Key:   message.Headers[i].Key,
			Value: message.Headers[i].Value,
		})
	}

	return kcronsumer.NewMessageBuilder().
		WithKey(message.Key).
		WithValue(message.Value).
		WithTopic(retryTopic).
		WithHeaders(headers).
		WithPartition(message.Partition).
		WithHighWatermark(message.HighWaterMark).
		Build()
}

func convertFromRetryableMessage(message kcronsumer.Message) Message {
	headers := make([]protocol.Header, 0, len(message.Headers))
	for i := range message.Headers {
		headers = append(headers, protocol.Header{
			Key:   message.Headers[i].Key,
			Value: message.Headers[i].Value,
		})
	}

	return Message{
		Topic:         message.Topic,
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Headers:       headers,
		Time:          message.Time,
	}
}

func (c *consumer) consume() {
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case <-c.quit:
			return
		default:
			message, err := c.r.ReadMessage(context.Background())
			if err != nil {
				continue
			}

			c.messageCh <- Message(message)
		}
	}
}

func (c *consumer) Stop() error {
	var err error
	c.once.Do(func() {
		c.cronsumer.Stop()
		err = c.r.Close()
		c.quit <- struct{}{}
		close(c.messageCh)
		c.wg.Wait()
	})

	return err
}
