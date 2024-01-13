package kafka

import (
	"context"
	"sync"
	"time"

	otelkafkakonsumer "github.com/Trendyol/otel-kafka-konsumer"

	"go.opentelemetry.io/otel/propagation"

	"github.com/prometheus/client_golang/prometheus"

	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	// Consume starts consuming
	Consume()

	// Pause function pauses consumer, it is stop consuming new messages
	Pause()

	// Resume function resumes consumer, it is start to working
	Resume()

	// WithLogger for injecting custom log implementation
	WithLogger(logger LoggerInterface)

	// Stop for graceful shutdown. In order to avoid data loss, you have to call it!
	Stop() error
}

type Reader interface {
	FetchMessage(ctx context.Context, msg *kafka.Message) error
	Close() error
	CommitMessages(messages []kafka.Message) error
}

type base struct {
	cronsumer                 kcronsumer.Cronsumer
	api                       API
	logger                    LoggerInterface
	propagator                propagation.TextMapPropagator
	context                   context.Context
	r                         Reader
	cancelFn                  context.CancelFunc
	metric                    *ConsumerMetric
	quit                      chan struct{}
	messageProcessedStream    chan struct{}
	incomingMessageStream     chan *IncomingMessage
	singleConsumingStream     chan *Message
	batchConsumingStream      chan []*Message
	retryTopic                string
	subprocesses              subprocesses
	wg                        sync.WaitGroup
	concurrency               int
	messageGroupDuration      time.Duration
	once                      sync.Once
	retryEnabled              bool
	transactionalRetry        bool
	distributedTracingEnabled bool
	pauseConsuming            bool
}

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	if cfg.BatchConfiguration != nil {
		return newBatchConsumer(cfg)
	}

	return newSingleConsumer(cfg)
}

func newBase(cfg *ConsumerConfig, messageChSize int) (*base, error) {
	log := NewZapLogger(cfg.LogLevel)

	reader, err := cfg.newKafkaReader()
	if err != nil {
		log.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	c := base{
		metric:                    &ConsumerMetric{},
		incomingMessageStream:     make(chan *IncomingMessage, messageChSize),
		quit:                      make(chan struct{}),
		concurrency:               cfg.Concurrency,
		retryEnabled:              cfg.RetryEnabled,
		transactionalRetry:        *cfg.TransactionalRetry,
		distributedTracingEnabled: cfg.DistributedTracingEnabled,
		logger:                    log,
		subprocesses:              newSubProcesses(),
		r:                         reader,
		messageGroupDuration:      cfg.MessageGroupDuration,
		messageProcessedStream:    make(chan struct{}, cfg.Concurrency),
		singleConsumingStream:     make(chan *Message, cfg.Concurrency),
		batchConsumingStream:      make(chan []*Message, cfg.Concurrency),
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
			close(c.incomingMessageStream)
			return
		default:
			if c.pauseConsuming {
				continue
			}

			m := &kafka.Message{}
			err := c.r.FetchMessage(c.context, m)
			if err != nil {
				if c.context.Err() != nil {
					continue
				}
				c.logger.Warnf("Message could not read, err %s", err.Error())
				continue
			}

			incomingMessage := &IncomingMessage{
				kafkaMessage: m,
				message:      fromKafkaMessage(m),
			}

			if c.distributedTracingEnabled {
				incomingMessage.message.Context = c.propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(m))
			}

			c.incomingMessageStream <- incomingMessage
		}
	}
}

func (c *base) Pause() {
	c.logger.Info("Consumer is paused!")
	c.pauseConsuming = true
	c.cancelFn()
}

func (c *base) Resume() {
	c.logger.Info("Consumer is resumed!")
	c.pauseConsuming = false
	c.context, c.cancelFn = context.WithCancel(context.Background())
}

func (c *base) WithLogger(logger LoggerInterface) {
	c.logger = logger
}

func (c *base) Stop() error {
	c.logger.Info("Stop called!")

	var err error
	c.once.Do(func() {
		c.subprocesses.Stop()
		c.cancelFn()
		c.quit <- struct{}{}
		c.wg.Wait()
		err = c.r.Close()
	})

	return err
}
