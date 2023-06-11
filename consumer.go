package kafka

import (
	"context"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-konsumer/instrumentation"
	"github.com/segmentio/kafka-go"
	"sync"
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
	retryTopic   string

	logger LoggerInterface

	cancelFn context.CancelFunc
	context  context.Context

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
		subprocesses: newSubProcesses(),
		r:            reader,
	}
	c.context, c.cancelFn = context.WithCancel(context.Background())

	if cfg.RetryEnabled {
		c.retryTopic = cfg.RetryConfiguration.Topic
		c.cronsumer = cronsumer.New(cfg.newCronsumerConfig(), c.retryFn)
		c.subprocesses.Add(c.cronsumer)
	}

	if cfg.APIEnabled {
		c.api = NewAPI(cfg)
		c.subprocesses.Add(c.api)
	}

	return &c, nil
}

func (c *consumer) Consume() {
	go c.subprocesses.Start()
	go c.consume()

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

func (c *consumer) Stop() error {
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

func (c *consumer) consume() {
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case <-c.quit:
			return
		default:
			message, err := c.r.FetchMessage(c.context)
			if err != nil {
				if c.context.Err() != nil {
					continue
				}
				c.logger.Errorf("Message could not read, err %s", err.Error())
				continue
			}

			c.messageCh <- Message(message)
		}
	}
}

func (c *consumer) process(message Message) {
	consumeErr := c.consumeFn(message)

	if consumeErr != nil && c.retryEnabled {
		c.logger.Warnf("Consume Function Err %s, Message is sending to exception/retry topic %s", consumeErr.Error(), c.retryTopic)

		retryableMsg := message.toRetryableMessage(c.retryTopic)

		if retryErr := c.retryFn(retryableMsg); retryErr != nil {
			if produceErr := c.cronsumer.Produce(retryableMsg); produceErr != nil {
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

func (c *consumer) retryFn(message kcronsumer.Message) error {
	return c.consumeFn(toMessage(message))
}
