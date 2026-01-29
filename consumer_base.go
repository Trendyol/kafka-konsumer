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

	// ResumeFromLatestOffset resumes consumer from the latest offset of the topic
	// This is useful when you want to skip messages that arrived during pause
	// It works idempotent under the hood
	// Calling with multiple goroutines is safe
	ResumeFromLatestOffset() error

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
	Config() kafka.ReaderConfig
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
	deadLetterTopic           string
	distributedTracingEnabled bool
	consumerState             state
	metricPrefix              string
	mu                        sync.Mutex
	consumerCfg               *ConsumerConfig
	deadLetterProducer        Producer
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
		metric:                    &ConsumerMetric{},
		incomingMessageStream:     make(chan *IncomingMessage, messageChSize),
		quit:                      make(chan struct{}),
		pause:                     make(chan struct{}),
		concurrency:               cfg.Concurrency,
		retryEnabled:              cfg.RetryEnabled,
		transactionalRetry:        *cfg.TransactionalRetry,
		deadLetterTopic:           cfg.DeadLetterTopic,
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
		consumerCfg:               cfg,
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

func (c *base) ResumeFromLatestOffset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.consumerState == stateRunning {
		c.logger.Debug("Consumer is already running mode!")
		return nil
	}

	// Get reader config to access topic and brokers
	readerConfig := c.r.Config()

	// Only works with consumer groups
	if readerConfig.GroupID == "" {
		return fmt.Errorf("ResumeFromLatestOffset only works with consumer groups")
	}

	c.logger.Info("Getting latest offsets before resume...")

	// Get latest offsets for all partitions
	latestOffsets, err := c.getLatestOffsets(readerConfig)
	if err != nil {
		return fmt.Errorf("failed to get latest offsets: %w", err)
	}

	c.logger.Infof("Latest offsets retrieved: %v", latestOffsets)

	// Create messages for commit
	// Note: latestOffsets contains high watermarks (next offset to be written)
	// To resume from that point, we need to commit the last consumed offset
	// which is highWatermark - 1 (because commit semantics: "I read offset N, next is N+1")
	offsetMarkers := make([]kafka.Message, 0, len(latestOffsets))
	for partition, highWatermark := range latestOffsets {
		// Skip if partition is empty (no messages written yet)
		if highWatermark == 0 {
			c.logger.Infof("Partition %d is empty, skipping commit", partition)
			continue
		}

		// Commit the last written offset (highWatermark - 1)
		// This tells Kafka: "I've read up to offset (highWatermark-1), next read from highWatermark"
		lastWrittenOffset := highWatermark - 1
		offsetMarkers = append(offsetMarkers, kafka.Message{
			Topic:     readerConfig.Topic,
			Partition: partition,
			Offset:    lastWrittenOffset,
		})
		c.logger.Debugf("Partition %d: high watermark=%d, committing offset=%d",
			partition, highWatermark, lastWrittenOffset)
	}

	if len(offsetMarkers) == 0 {
		c.logger.Warn("No offsets to commit (all partitions are empty), skipping commit")
		// Still need to resume the consumer
		c.pause = make(chan struct{})
		c.context, c.cancelFn = context.WithCancel(context.Background())
		c.consumerState = stateRunning

		c.wg.Add(1)
		go c.startConsume()

		c.logger.Info("Consumer resumed (no offset commit needed)!")
		return nil
	}

	// Commit using the existing Reader (it's still a member of the consumer group)
	// Even though consumer is paused, the Reader maintains consumer group membership
	c.logger.Info("Committing latest offsets via existing Reader...")
	err = c.r.CommitMessages(offsetMarkers)
	if err != nil {
		return fmt.Errorf("failed to commit latest offsets: %w", err)
	}

	// Close the Reader - this will flush any pending commits before closing
	// The Reader's Close() method ensures buffered commits are sent to Kafka
	c.logger.Info("Closing Reader (this flushes pending commits)...")
	if err := c.r.Close(); err != nil {
		return fmt.Errorf("failed to close reader: %w", err)
	}

	// Create new reader with the same configuration
	// When it joins the consumer group, it will fetch the offsets we just committed
	c.logger.Info("Creating new Reader...")
	newReader, err := c.consumerCfg.newKafkaReader(c.logger)
	if err != nil {
		return fmt.Errorf("failed to recreate reader: %w", err)
	}
	c.r = newReader

	c.logger.Info("Reader recreated successfully")

	// Resume consumer
	c.pause = make(chan struct{})
	c.context, c.cancelFn = context.WithCancel(context.Background())
	c.consumerState = stateRunning

	c.wg.Add(1)
	go c.startConsume()

	c.logger.Info("Consumer resumed from latest offsets!")

	return nil
}

func (c *base) getLatestOffsets(readerConfig kafka.ReaderConfig) (map[int]int64, error) {
	const (
		maxRetries      = 10
		initialBackoff  = 100 * time.Millisecond
		maxBackoff      = 10 * time.Second
		backoffMultiple = 2
	)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * initialBackoff * backoffMultiple
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			c.logger.Infof("Retrying getLatestOffsets (attempt %d/%d) after %v...", attempt+1, maxRetries, backoff)
			time.Sleep(backoff)
		}

		offsets, err := c.tryGetLatestOffsets(readerConfig)
		if err == nil {
			if len(offsets) == 0 {
				lastErr = fmt.Errorf("no partition offsets retrieved")
				c.logger.Warnf("Attempt %d/%d: %v", attempt+1, maxRetries, lastErr)
				continue
			}
			return offsets, nil
		}

		lastErr = err
		c.logger.Warnf("Attempt %d/%d failed: %v", attempt+1, maxRetries, err)
	}

	return nil, fmt.Errorf("failed to get latest offsets after %d attempts: %w", maxRetries, lastErr)
}

func (c *base) tryGetLatestOffsets(readerConfig kafka.ReaderConfig) (map[int]int64, error) {
	// Use the custom dialer from ReaderConfig (supports TLS, SASL, etc.)
	dialer := readerConfig.Dialer
	if dialer == nil {
		dialer = kafka.DefaultDialer
	}

	// Try to connect to any available broker
	var conn *kafka.Conn
	var err error
	for _, broker := range readerConfig.Brokers {
		conn, err = dialer.Dial("tcp", broker)
		if err == nil {
			break
		}
		c.logger.Debugf("Failed to dial broker %s: %v", broker, err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to dial any broker: %w", err)
	}
	defer conn.Close()

	// Get partition list for the topic
	partitions, err := conn.ReadPartitions(readerConfig.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found for topic %s", readerConfig.Topic)
	}

	latestOffsets := make(map[int]int64)
	var failedPartitions []int

	// Get latest offset for each partition
	for _, partition := range partitions {
		// Try to connect to partition leader using multiple brokers
		var partConn *kafka.Conn
		var dialErr error
		for _, broker := range readerConfig.Brokers {
			partConn, dialErr = dialer.DialLeader(context.Background(), "tcp",
				broker, readerConfig.Topic, partition.ID)
			if dialErr == nil {
				break
			}
		}
		if dialErr != nil {
			c.logger.Debugf("Failed to dial partition %d leader from any broker: %v", partition.ID, dialErr)
			failedPartitions = append(failedPartitions, partition.ID)
			continue
		}

		// Get first and last offsets
		_, lastOffset, err := partConn.ReadOffsets()
		if err != nil {
			c.logger.Debugf("Failed to read offsets for partition %d: %v", partition.ID, err)
			partConn.Close()
			failedPartitions = append(failedPartitions, partition.ID)
			continue
		}

		latestOffsets[partition.ID] = lastOffset
		partConn.Close()
	}

	// If we failed to get offsets for any partition, return error for retry
	if len(failedPartitions) > 0 {
		return nil, fmt.Errorf("failed to get offsets for partitions %v", failedPartitions)
	}

	return latestOffsets, nil
}

func initializeDeadLetterProducer(cfg *ConsumerConfig) (Producer, error) {
	deadLetterTopic := cfg.DeadLetterTopic
	if deadLetterTopic == "" {
		deadLetterTopic = cfg.RetryConfiguration.DeadLetterTopic
	}

	deadLetterProducer, err := NewProducer(&ProducerConfig{
		Writer: WriterConfig{
			Topic:   deadLetterTopic,
			Brokers: cfg.Reader.Brokers,
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
