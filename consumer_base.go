package kafka

import (
	"context"
	"sync"

	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Consume()
	WithLogger(logger LoggerInterface)
	Stop() error
}

type base struct {
	r           *kafka.Reader
	wg          sync.WaitGroup
	once        sync.Once
	messageCh   chan Message
	quit        chan struct{}
	concurrency int

	cronsumer kcronsumer.Cronsumer

	retryEnabled bool
	retryTopic   string

	cancelFn context.CancelFunc
	context  context.Context

	logger LoggerInterface

	api          API
	subprocesses subprocesses
}

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	if cfg.BatchConfiguration != nil {
		return newBatchConsumer(cfg)
	}

	return newSingleConsumer(cfg)
}

func newBase(cfg *ConsumerConfig) (*base, error) {
	log := NewZapLogger(cfg.LogLevel)

	reader, err := cfg.newKafkaReader()
	if err != nil {
		log.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	c := base{
		messageCh:    make(chan Message, cfg.Concurrency),
		quit:         make(chan struct{}),
		concurrency:  cfg.Concurrency,
		retryEnabled: cfg.RetryEnabled,
		logger:       log,
		subprocesses: newSubProcesses(),
		r:            reader,
	}

	c.context, c.cancelFn = context.WithCancel(context.Background())

	return &c, nil
}

func (c *base) setupCronsumer(cfg *ConsumerConfig, retryFn func(kcronsumer.Message) error) {
	c.logger.Debug("Initializing Cronsumer")
	c.retryTopic = cfg.RetryConfiguration.Topic
	c.cronsumer = cronsumer.New(cfg.newCronsumerConfig(), retryFn)
	c.subprocesses.Add(c.cronsumer)
}

func (c *base) setupAPI(cfg *ConsumerConfig) {
	c.logger.Debug("Initializing API")
	c.api = NewAPI(cfg)
	c.subprocesses.Add(c.api)
}

func (c *base) startConsume() {
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

func (c *base) WithLogger(logger LoggerInterface) {
	c.logger = logger
}

func (c *base) Stop() error {
	c.logger.Debug("Stop called!")
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
