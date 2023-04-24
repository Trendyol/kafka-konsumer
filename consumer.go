package kafka

import (
	"context"
	"sync"

	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type Consumer interface {
	Consume()
	WithLogger(logger LoggerInterface)
	Stop() error
}

type consumer struct {
	r           *kafka.Reader
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

	logger LoggerInterface
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	c := consumer{
		messageCh:    make(chan Message, cfg.Concurrency),
		quit:         make(chan struct{}),
		concurrency:  cfg.Concurrency,
		consumeFn:    cfg.ConsumeFn,
		retryEnabled: cfg.RetryEnabled,
		logger:       NewZapLogger(cfg.LogLevel),
	}

	var err error
	if c.r, err = cfg.newKafkaReader(); err != nil {
		c.logger.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	if cfg.RetryEnabled {
		c.logger.Debug("Konsumer retry enabled mode active!")
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
			c.logger.Debug("Setting cronsumer SASL configurations...")
			kcronsumerCfg.SASL.Enabled = true
			kcronsumerCfg.SASL.AuthType = string(cfg.SASL.Type)
			kcronsumerCfg.SASL.Username = cfg.SASL.Username
			kcronsumerCfg.SASL.Password = cfg.SASL.Password
		}

		if !cfg.TLS.IsEmpty() {
			c.logger.Debug("Setting cronsumer TLS configurations...")
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
	c.logger.Info("Consuming is starting!")
	if c.retryEnabled {
		c.logger.Debug("Cronsumer is starting!")
		c.cronsumer.Start()
	}

	go c.consume()

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for message := range c.messageCh {
				err := c.consumeFn(message)

				if err != nil && c.retryEnabled {
					retryableMsg := convertToRetryableMessage(c.retryTopic, message)
					if err := c.retryFn(retryableMsg); err != nil {
						if err = c.cronsumer.Produce(retryableMsg); err != nil {
							c.logger.Errorf("Error producing message %s to exception/retry topic %v",
								string(retryableMsg.Value), err.Error())
						}
					}
				}
				if err = c.r.CommitMessages(context.Background(), kafka.Message(message)); err != nil {
					c.logger.Errorf("Error Committing message %s",
						string(message.Value))
				}
			}
		}()
	}
}

func (c *consumer) WithLogger(logger LoggerInterface) {
	c.logger = logger
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
	c.logger.Debug("Consuming is starting")

	for {
		select {
		case <-c.quit:
			return
		default:
			message, err := c.r.FetchMessage(context.Background())
			if err != nil {
				c.logger.Errorf("Message could not read, err %s", err.Error())
				continue
			}

			c.messageCh <- Message(message)
		}
	}
}

func (c *consumer) Stop() error {
	var err error
	c.once.Do(func() {
		if c.retryEnabled {
			c.cronsumer.Stop()
		}
		c.quit <- struct{}{}
		close(c.messageCh)
		err = c.r.Close()
		c.wg.Wait()
	})

	return err
}
