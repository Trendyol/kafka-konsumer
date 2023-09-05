package kafka

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

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
	cronsumer    kcronsumer.Cronsumer
	api          API
	logger       LoggerInterface
	metric       *ConsumerMetric
	context      context.Context
	messageCh    chan Message
	quit         chan struct{}
	commitReqCh  chan []kafka.Message
	cancelFn     context.CancelFunc
	r            *kafka.Reader
	retryTopic   string
	subprocesses subprocesses
	wg           sync.WaitGroup
	concurrency  int
	once         sync.Once
	retryEnabled bool
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
		metric:       &ConsumerMetric{},
		messageCh:    make(chan Message, cfg.Concurrency),
		quit:         make(chan struct{}),
		commitReqCh:  make(chan []kafka.Message),
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

func (c *base) setupAPI(cfg *ConsumerConfig, consumerMetric *ConsumerMetric) {
	c.logger.Debug("Initializing API")

	var metricCollectors []prometheus.Collector
	if cfg.RetryEnabled {
		metricCollectors = c.cronsumer.GetMetricCollectors()
	}

	c.api = NewAPI(cfg, consumerMetric, metricCollectors...)
	c.subprocesses.Add(c.api)
}

func (c *base) startConsume() {
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
		close(c.commitReqCh)
		err = c.r.Close()
	})

	return err
}
