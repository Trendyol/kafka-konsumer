package kafka

import (
	"context"
	"sync"

	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-konsumer/instrumentation"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Consume()
	WithLogger(logger LoggerInterface)
	Stop() error
}

type subprocess interface {
	Start()
	Stop()
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

	cancelFn     context.CancelFunc
	api          API
	subprocesses []subprocess
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	logger := NewZapLogger(cfg.LogLevel)
	reader, err := cfg.newKafkaReader()
	if err != nil {
		logger.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	c := consumer{
		messageCh:    make(chan Message, cfg.Concurrency),
		quit:         make(chan struct{}),
		concurrency:  cfg.Concurrency,
		consumeFn:    cfg.ConsumeFn,
		retryEnabled: cfg.RetryEnabled,
		logger:       logger,
		subprocesses: []subprocess{},
		r:            reader,
	}

	if cfg.RetryEnabled {
		c.logger.Debug("Konsumer retry enabled mode active!")
		kcronsumerCfg := kcronsumer.Config{
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
			c.logger.Debug("Setting cronsumer SASL configurations...")
			kcronsumerCfg.SASL.Enabled = true
			kcronsumerCfg.SASL.AuthType = string(cfg.RetryConfiguration.SASL.Type)
			kcronsumerCfg.SASL.Username = cfg.RetryConfiguration.SASL.Username
			kcronsumerCfg.SASL.Password = cfg.RetryConfiguration.SASL.Password
			kcronsumerCfg.SASL.Rack = cfg.RetryConfiguration.Rack
		}

		if !cfg.RetryConfiguration.TLS.IsEmpty() {
			c.logger.Debug("Setting cronsumer TLS configurations...")
			kcronsumerCfg.SASL.RootCAPath = cfg.RetryConfiguration.TLS.RootCAPath
			kcronsumerCfg.SASL.IntermediateCAPath = cfg.RetryConfiguration.TLS.IntermediateCAPath
		}

		c.retryTopic = cfg.RetryConfiguration.Topic
		c.retryFn = func(message kcronsumer.Message) error {
			consumeErr := c.consumeFn(ToMessage(message))
			instrumentation.TotalProcessedRetryableMessagesCounter.Inc()
			return consumeErr
		}

		c.cronsumer = cronsumer.New(&kcronsumerCfg, c.retryFn)
		c.subprocesses = append(c.subprocesses, c.cronsumer)
	}

	if cfg.APIEnabled {
		c.logger.Debug("Metrics API Enabled!")
		c.api = NewAPI(cfg)
		c.subprocesses = append(c.subprocesses, c.api)
	}

	return &c, nil
}

func (c *consumer) Consume() {
	go c.startSubprocesses()

	ctx, cancel := context.WithCancel(context.Background())
	c.cancelFn = cancel

	go c.consume(ctx)

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for message := range c.messageCh {
				err := c.consumeFn(message)
				instrumentation.TotalProcessedMessagesCounter.Inc()

				if err != nil && c.retryEnabled {
					c.logger.Warnf("Consume Function Err %s, Message is sending to exception/retry topic %s", err.Error(), c.retryTopic)

					retryableMsg := message.ToRetryableMessage(c.retryTopic)
					if err = c.retryFn(retryableMsg); err != nil {
						if err = c.cronsumer.Produce(retryableMsg); err != nil {
							c.logger.Errorf("Error producing message %s to exception/retry topic %v",
								string(retryableMsg.Value), err.Error())
						}
					}
				}

				if err = c.r.CommitMessages(context.Background(), kafka.Message(message)); err != nil {
					c.logger.Errorf("Error Committing message %s", string(message.Value))
				}
			}
		}()
	}
}

func (c *consumer) Stop() error {
	c.logger.Debug("Consuming is closing!")
	var err error
	c.once.Do(func() {
		c.stopSubprocesses()
		c.cancelFn()
		c.quit <- struct{}{}
		close(c.messageCh)
		c.wg.Wait()
		err = c.r.Close()
	})

	return err
}

func (c *consumer) WithLogger(logger LoggerInterface) {
	c.logger = logger
}

func (c *consumer) startSubprocesses() {
	for i := range c.subprocesses {
		c.subprocesses[i].Start()
	}
}

func (c *consumer) stopSubprocesses() {
	for i := range c.subprocesses {
		c.subprocesses[i].Stop()
	}
}

func (c *consumer) consume(ctx context.Context) {
	c.logger.Debug("Consuming is starting")
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case <-c.quit:
			return
		default:
			message, err := c.r.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				c.logger.Errorf("Message could not read, err %s", err.Error())
				continue
			}

			c.messageCh <- Message(message)
		}
	}
}
