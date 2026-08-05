package kafka

import (
	"context"
	"fmt"
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
	cronsumer                    kcronsumer.Cronsumer
	api                          API
	logger                       LoggerInterface
	propagator                   propagation.TextMapPropagator
	context                      context.Context
	r                            Reader
	cancelFn                     context.CancelFunc
	skipMessageByHeaderFn        SkipMessageByHeaderFn
	metric                       *ConsumerMetric
	pause                        chan struct{}
	quit                         chan struct{}
	messageProcessedStream       chan struct{}
	incomingMessageStream        chan *IncomingMessage
	singleConsumingStream        chan *Message
	batchConsumingStream         chan []*Message
	retryTopic                   string
	subprocesses                 subprocesses
	wg                           sync.WaitGroup
	concurrency                  int
	messageGroupDuration         time.Duration
	once                         sync.Once
	retryEnabled                 bool
	transactionalRetry           bool
	deadLetterTopic              string
	distributedTracingEnabled    bool
	consumerState                state
	metricPrefix                 string
	mu                           sync.Mutex
	consumerCfg                  *ConsumerConfig
	deadLetterProducer           Producer
	deadLetterProducerBatchBytes int64
}

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	if cfg.BatchConfiguration != nil {
		return newBatchConsumer(cfg)
	}

	return newSingleConsumer(cfg)
}

func newBase(cfg *ConsumerConfig, messageChSize int) (*base, error) {
	log := NewZapLogger(cfg.LogLevel)

	if err := verifyTopicOnStartup(cfg, log); err != nil {
		return nil, err
	}

	log.Infof("Topic [%s] verified successfully!", cfg.getTopics())

	reader, err := cfg.newKafkaReader(log)
	if err != nil {
		log.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	c := base{
		metric:                       &ConsumerMetric{},
		incomingMessageStream:        make(chan *IncomingMessage, messageChSize),
		quit:                         make(chan struct{}),
		pause:                        make(chan struct{}),
		concurrency:                  cfg.Concurrency,
		retryEnabled:                 cfg.RetryEnabled,
		transactionalRetry:           *cfg.TransactionalRetry,
		deadLetterTopic:              cfg.DeadLetterTopic,
		distributedTracingEnabled:    cfg.DistributedTracingEnabled,
		logger:                       log,
		subprocesses:                 newSubProcesses(),
		r:                            reader,
		messageGroupDuration:         cfg.MessageGroupDuration,
		messageProcessedStream:       make(chan struct{}, cfg.Concurrency),
		singleConsumingStream:        make(chan *Message, cfg.Concurrency),
		batchConsumingStream:         make(chan []*Message, cfg.Concurrency),
		consumerState:                stateRunning,
		skipMessageByHeaderFn:        cfg.SkipMessageByHeaderFn,
		metricPrefix:                 cfg.MetricPrefix,
		mu:                           sync.Mutex{},
		consumerCfg:                  cfg,
		deadLetterProducerBatchBytes: cfg.DeadLetterProducerBatchBytes,
	}

	if cfg.DistributedTracingEnabled {
		c.propagator = cfg.DistributedTracingConfiguration.Propagator
	}

	// Initialize dead letter producer if needed
	if cfg.DeadLetterTopic != "" || cfg.RetryConfiguration.DeadLetterTopic != "" {
		c.deadLetterProducer, err = initializeDeadLetterProducer(cfg)
		if err != nil {
			return nil, err
		}
	}

	c.context, c.cancelFn = context.WithCancel(context.Background())

	return &c, nil
}

func verifyTopicOnStartup(cfg *ConsumerConfig, logger LoggerInterface) error {
	kclient, err := newKafkaClient(cfg, logger)
	if err != nil {
		return err
	}
	exist, err := verifyTopics(kclient, cfg)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("topics %s does not exist, please check cluster authority etc", cfg.getTopics())
	}
	return nil
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

				c.metric.IncrementTotalErrorCountDuringFetchingMessage(1)
				//nolint:lll
				c.logger.Warnf("Message could not read, err %s, from topics %s with consumer group %s", err.Error(), c.consumerCfg.getTopics(), c.consumerCfg.Reader.GroupID)
				continue
			}

			incomingMessage := &IncomingMessage{
				kafkaMessage: m,
				message:      fromKafkaMessage(m),
			}

			if c.skipMessageByHeaderFn != nil && c.skipMessageByHeaderFn(m.Headers) {
				c.logger.Debugf("Message is not processed. Header filter applied. Headers: %v", incomingMessage.message.Headers.Pretty())
				if err = c.r.CommitMessages([]kafka.Message{*m}); err != nil {
					c.logger.Errorf("Commit Error %s,", err.Error())
				}
				continue
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

func initializeDeadLetterProducer(cfg *ConsumerConfig) (Producer, error) {
	deadLetterTopic := cfg.DeadLetterTopic
	if deadLetterTopic == "" {
		deadLetterTopic = cfg.RetryConfiguration.DeadLetterTopic
	}

	deadLetterProducer, err := NewProducer(&ProducerConfig{
		Writer: WriterConfig{
			Topic:                  deadLetterTopic,
			Brokers:                cfg.Reader.Brokers,
			AllowAutoTopicCreation: true,
			RequiredAcks:           cfg.RetryConfiguration.ProducerRequiredAcks,
			Compression:            cfg.RetryConfiguration.ProducerCompression,
			BatchBytes:             cfg.DeadLetterProducerBatchBytes,
		},
		LogLevel: cfg.LogLevel,
		SASL:     cfg.SASL,
		TLS:      cfg.TLS,
		ClientID: cfg.ClientID,
	})
	if err != nil {
		return nil, fmt.Errorf("error initializing producer for dead letter topic %s: %w", deadLetterTopic, err)
	}
	return deadLetterProducer, nil
}

func (c *base) sendToDeadLetterWithBackoff(messages ...Message) error {
	if c.deadLetterProducerBatchBytes <= 0 {
		return c.produceDeadLetterBatchWithBackoff(messages...)
	}

	for _, chunk := range chunkMessagesByBytes(messages, c.deadLetterProducerBatchBytes) {
		if err := c.produceDeadLetterBatchWithBackoff(chunk...); err != nil {
			return fmt.Errorf("error producing direct dead letter chunk messages=%d approxBytes=%d batchBytesLimit=%d: %w",
				len(chunk), messagesTotalSize(chunk), c.deadLetterProducerBatchBytes, err)
		}
	}

	return nil
}

func (c *base) produceDeadLetterBatchWithBackoff(messages ...Message) error {
	var produceErr error

	for attempt := 1; attempt <= 5; attempt++ {
		produceErr = c.deadLetterProducer.ProduceBatch(context.Background(), messages)
		if produceErr == nil {
			return nil
		}
		c.logger.Warnf("Error producing messages to dead letter topic (attempt %d/%d): %v", attempt, 5, produceErr)
		time.Sleep((50 * time.Millisecond) * time.Duration(1<<attempt))
	}

	return produceErr
}

func chunkMessagesByBytes(messages []Message, limit int64) [][]Message {
	if limit <= 0 || len(messages) == 0 {
		return [][]Message{messages}
	}

	chunks := make([][]Message, 0, len(messages))
	currentChunkStart := 0
	currentChunkBytes := int64(0)

	for i := range messages {
		messageBytes := int64(messages[i].TotalSize())
		if currentChunkBytes > 0 && currentChunkBytes+messageBytes > limit {
			chunks = append(chunks, messages[currentChunkStart:i])
			currentChunkStart = i
			currentChunkBytes = 0
		}

		currentChunkBytes += messageBytes
	}

	if currentChunkStart < len(messages) {
		chunks = append(chunks, messages[currentChunkStart:])
	}

	return chunks
}

func messagesTotalSize(messages []Message) int64 {
	total := int64(0)
	for i := range messages {
		total += int64(messages[i].TotalSize())
	}
	return total
}

func (c *base) retryWithBackoff(retryableMessage ...kcronsumer.Message) error {
	var produceErr error

	for attempt := 1; attempt <= 5; attempt++ {
		produceErr = c.cronsumer.ProduceBatch(retryableMessage)
		if produceErr == nil {
			return nil
		}
		c.logger.Warnf("Error producing message (attempt %d/%d): %v", attempt, 5, produceErr)
		time.Sleep((50 * time.Millisecond) * time.Duration(1<<attempt))
	}

	return produceErr
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
		if c.deadLetterProducer != nil {
			c.deadLetterProducer.Close()
		}
		err = c.r.Close()
	})

	return err
}

func drainTimer(t *time.Timer) {
	select {
	case <-t.C:
	default:
	}
}
