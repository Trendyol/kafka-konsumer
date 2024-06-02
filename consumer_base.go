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
	// It works idempotent under the hood
	// Calling with multiple goroutines is safe
	Pause()

	// Resume function resumes consumer, it is start to working
	// It works idempotent under the hood
	// Calling with multiple goroutines is safe
	Resume()

	// GetMetricCollectors for the purpose of making metric collectors available.
	// You can register these collectors on your own http server.
	// Please look at the examples/with-metric-collector directory.
	GetMetricCollectors() []prometheus.Collector

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

type state string

const (
	stateRunning state = "running"
	statePaused  state = "paused"
)

type base struct {
	cronsumer                 kcronsumer.Cronsumer
	api                       API
	logger                    LoggerInterface
	propagator                propagation.TextMapPropagator
	context                   context.Context
	r                         Reader
	cancelFn                  context.CancelFunc
	skipMessageByHeaderFn     SkipMessageByHeaderFn
	metric                    *ConsumerMetric
	pause                     chan struct{}
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
	consumerState             state
	metricPrefix              string
	mu                        sync.Mutex
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
		pause:                     make(chan struct{}),
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
		consumerState:             stateRunning,
		skipMessageByHeaderFn:     cfg.SkipMessageByHeaderFn,
		metricPrefix:              cfg.MetricPrefix,
		mu:                        sync.Mutex{},
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

func (c *base) GetMetricCollectors() []prometheus.Collector {
	var metricCollectors []prometheus.Collector

	if c.retryEnabled {
		metricCollectors = c.cronsumer.GetMetricCollectors()
	}

	metricCollectors = append(metricCollectors, NewMetricCollector(c.metricPrefix, c.metric))

	return metricCollectors
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
		case <-c.pause:
			c.logger.Debug("startConsume exited!")
			return
		case <-c.quit:
			close(c.incomingMessageStream)
			return
		default:
			m := &kafka.Message{}
			err := c.r.FetchMessage(c.context, m)
			if err != nil {
				c.logger.Debug("c.r.FetchMessage ", err.Error())
				if c.context.Err() != nil {
					continue
				}

				c.metric.TotalErrorCountDuringFetchingMessage++
				c.logger.Warnf("Message could not read, err %s", err.Error())
				continue
			}

			if c.skipMessageByHeaderFn != nil && c.skipMessageByHeaderFn(m.Headers) {
				c.logger.Infof("Message is not processed. Header filter applied. Headers: %v", m.Headers)
				if err = c.r.CommitMessages([]kafka.Message{*m}); err != nil {
					c.logger.Errorf("Commit Error %s,", err.Error())
				}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.consumerState == statePaused {
		c.logger.Debug("Consumer is already paused mode!")
		return
	}

	c.logger.Infof("Consumer is paused!")

	c.cancelFn()

	c.pause <- struct{}{}

	c.consumerState = statePaused
}

func (c *base) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.consumerState == stateRunning {
		c.logger.Debug("Consumer is already running mode!")
		return
	}

	c.logger.Info("Consumer is resumed!")

	c.pause = make(chan struct{})
	c.context, c.cancelFn = context.WithCancel(context.Background())
	c.consumerState = stateRunning

	c.wg.Add(1)
	go c.startConsume()
}

func (c *base) WithLogger(logger LoggerInterface) {
	c.logger = logger
}

func (c *base) Stop() error {
	c.logger.Info("Stop is called!")

	var err error
	c.once.Do(func() {
		c.subprocesses.Stop()
		c.cancelFn()

		// In order to save cpu, we break startConsume loop in pause mode.
		// If consumer is pause mode and Stop is called
		// We need to close incomingMessageStream, because c.wg.Wait() blocks indefinitely.
		if c.consumerState == stateRunning {
			c.quit <- struct{}{}
		} else if c.consumerState == statePaused {
			close(c.incomingMessageStream)
		}

		c.wg.Wait()
		err = c.r.Close()
	})

	return err
}
