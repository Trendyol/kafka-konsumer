package kafka

import (
	"context"
	"sync"
	"unsafe"

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
	subprocesses subprocesses
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	log := NewZapLogger(cfg.LogLevel)
	reader, err := cfg.newKafkaReader()
	if err != nil {
		log.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	c := consumer{
		messageCh:    make(chan Message, cfg.Concurrency),
		quit:         make(chan struct{}),
		concurrency:  cfg.Concurrency,
		consumeFn:    cfg.ConsumeFn,
		retryEnabled: cfg.RetryEnabled,
		logger:       log,
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
			toMessage := (*Message)(unsafe.Pointer(&message))
			consumeErr := c.consumeFn(*toMessage)
			return consumeErr
		}

		c.cronsumer = cronsumer.New(&kcronsumerCfg, c.retryFn)
		c.subprocesses.Add(c.cronsumer)
	}

	if cfg.APIEnabled {
		c.logger.Debug("Metrics API Enabled!")
		c.api = NewAPI(cfg)
		c.subprocesses.Add(c.api)
	}

	return &c, nil
}

func (c *consumer) Consume() {
	go c.subprocesses.Start()

	ctx, cancel := context.WithCancel(context.Background())
	c.cancelFn = cancel

	go c.consume(ctx)

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for message := range c.messageCh {
				c.process(message)
			}
		}()
	}
}

func (c *consumer) process(message Message) {
	consumeErr := c.consumeFn(message)

	if consumeErr != nil && c.retryEnabled {
		c.logger.Warnf("Consume Function Err %s, Message is sending to exception/retry topic %s", consumeErr.Error(), c.retryTopic)

		retryableMsg := (*kcronsumer.Message)(unsafe.Pointer(&message))
		retryableMsg.Topic = c.retryTopic

		if retryErr := c.retryFn(*retryableMsg); retryErr != nil {
			if produceErr := c.cronsumer.Produce(*retryableMsg); produceErr != nil {
				c.logger.Errorf("Error producing message %s to exception/retry topic %v",
					string(retryableMsg.Value), produceErr.Error())
			}
		}

	}

	commitErr := c.r.CommitMessages(context.Background(), kafka.Message(message))
	if commitErr != nil {
		c.logger.Errorf("Error Committing message %s", string(message.Value))
		return
	}

	if consumeErr != nil {
		instrumentation.TotalProcessedRetryableMessagesCounter.Inc()
	}

	instrumentation.TotalProcessedMessagesCounter.Inc()
}

func (c *consumer) Stop() error {
	c.logger.Debug("Consuming is closing!")
	var err error
	c.once.Do(func() {
		c.subprocesses.Stop()
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
