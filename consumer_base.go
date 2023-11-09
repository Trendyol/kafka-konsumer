package kafka

import (
	"context"
	"sync"

	"github.com/Trendyol/otel-kafka-konsumer"
	"go.opentelemetry.io/otel/propagation"

	"github.com/prometheus/client_golang/prometheus"

	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	// Consume starts consuming
	Consume()

	// WithLogger for injecting custom log implementation
	WithLogger(logger LoggerInterface)

	// Stop for graceful shutdown. In order to avoid data loss, you have to call it!
	Stop() error
}

type Reader interface {
	ReadMessage(ctx context.Context) (*kafka.Message, error)
	Close() error
}

type base struct {
	cronsumer                 kcronsumer.Cronsumer
	api                       API
	logger                    LoggerInterface
	metric                    *ConsumerMetric
	context                   context.Context
	messageCh                 chan *Message
	quit                      chan struct{}
	cancelFn                  context.CancelFunc
	r                         Reader
	retryTopic                string
	subprocesses              subprocesses
	wg                        sync.WaitGroup
	concurrency               int
	once                      sync.Once
	retryEnabled              bool
	distributedTracingEnabled bool
	propagator                propagation.TextMapPropagator
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
		metric:                    &ConsumerMetric{},
		messageCh:                 make(chan *Message, cfg.Concurrency),
		quit:                      make(chan struct{}),
		concurrency:               cfg.Concurrency,
		retryEnabled:              cfg.RetryEnabled,
		distributedTracingEnabled: cfg.DistributedTracingEnabled,
		logger:                    log,
		subprocesses:              newSubProcesses(),
		r:                         reader,
	}

	if cfg.DistributedTracingEnabled {
		c.propagator = cfg.DistributedTracingConfiguration.Propagator
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
			message, err := c.r.ReadMessage(c.context)
			if err != nil {
				if c.context.Err() != nil {
					continue
				}
				c.logger.Errorf("Message could not read, err %s", err.Error())
				continue
			}

			consumedMessage := fromKafkaMessage(message)
			if c.distributedTracingEnabled {
				consumedMessage.Context = c.propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(message))
			}

			c.messageCh <- consumedMessage
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
