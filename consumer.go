package kafka

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type consumer struct {
	*base

	consumeFn    func(*Message) error
	workStream   chan processRequest
	resultStream chan processResult
	stopStream   chan struct{}
	streamsOnce  sync.Once
	stopOnce     sync.Once
	workersWG    sync.WaitGroup
}

func (c *consumer) Pause() {
	c.base.Pause()
}

func (c *consumer) Resume() {
	c.base.Resume()
}

func newSingleConsumer(cfg *ConsumerConfig) (Consumer, error) {
	consumerBase, err := newBase(cfg, cfg.Concurrency)
	if err != nil {
		return nil, err
	}

	c := consumer{
		base:      consumerBase,
		consumeFn: cfg.ConsumeFn,
	}
	c.deferHeaderCommit = true
	c.initializeStreams()

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, c.runKonsumerFn)
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (c *consumer) runKonsumerFn(message kcronsumer.Message) error {
	msg := toMessage(message)

	consumeErr := c.consumeFn(msg)
	// Without a dead letter producer, message follows the normal retry flow
	if consumeErr == nil || !msg.SendDirectToDeadLetter || c.deadLetterProducer == nil {
		return consumeErr
	}

	if err := c.sendToDeadLetterWithBackoff(msg.toDeadLetterMessage(consumeErr)); err != nil {
		c.logger.Errorf("Error producing retried message to dead letter topic, message will be retried. Error: %s", err.Error())
		return consumeErr
	}

	return nil
}

func (c *consumer) GetMetricCollectors() []prometheus.Collector {
	return c.base.GetMetricCollectors()
}

func (c *consumer) Consume() {
	c.initializeStreams()
	go c.subprocesses.Start()

	c.wg.Add(1)
	go c.startConsume()

	c.setupConcurrentWorkers()

	c.wg.Add(1)
	go c.startBatch()
}

func (c *consumer) startBatch() {
	defer c.wg.Done()
	c.initializeStreams()
	defer func() {
		close(c.workStream)
		c.workersWG.Wait()
		close(c.resultStream)
	}()

	flushTimer := time.NewTimer(c.messageGroupDuration)
	defer flushTimer.Stop()

	messages := make([]processRequest, 0, c.concurrency)
	commitMessages := make([]kafka.Message, 0, c.concurrency)
	blocked := false

	flushBatch := func(reason string) {
		if len(messages) == 0 || blocked {
			return
		}

		if err := c.consume(&messages, &commitMessages); err != nil {
			blocked = true
			return
		}

		c.logger.Debugf("[singleConsumer] Flushed batch, reason=%s", reason)

		// After flushing, we always reset the timer
		// But first we need to stop it and drain any event that might be pending
		if !flushTimer.Stop() {
			drainTimer(flushTimer)
		}

		// Now reset to start a new "rolling" interval
		flushTimer.Reset(c.messageGroupDuration)
	}

	for {
		select {
		case <-flushTimer.C:
			flushBatch("time-based (rolling timer)")
		case msg, ok := <-c.incomingMessageStream:
			if !ok {
				flushBatch("channel-closed (final flush)")
				return
			}
			if blocked {
				continue // Drain fetched messages during shutdown without committing them.
			}

			messages = append(messages, processRequest{message: msg.message, skip: msg.skip})
			commitMessages = append(commitMessages, *msg.kafkaMessage)

			if len(messages) == c.concurrency {
				flushBatch("message-count-limit")
			} else {
				// Rolling timer logic: reset the timer each time we get a new message
				// Because we "stop" it, we might need to drain the channel
				if !flushTimer.Stop() {
					drainTimer(flushTimer)
				}
				flushTimer.Reset(c.messageGroupDuration)
			}
		}
	}
}

func (c *consumer) setupConcurrentWorkers() {
	c.initializeStreams()
	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		c.workersWG.Add(1)
		go func() {
			defer c.wg.Done()
			defer c.workersWG.Done()
			for request := range c.workStream {
				var err error
				if !request.skip {
					err = c.process(request.message)
				}
				c.resultStream <- processResult{err: err}
			}
		}()
	}
}

func (c *consumer) consume(messages *[]processRequest, commitMessages *[]kafka.Message) error {
	messageList := *messages

	// Send the messages to process
	for _, message := range messageList {
		c.workStream <- message
	}

	// Wait the messages to be processed
	var batchErr error
	for range messageList {
		result := <-c.resultStream
		if result.err != nil {
			batchErr = errors.Join(batchErr, result.err)
			var failure *processError
			if errors.As(result.err, &failure) && len(failure.Stack) != 0 {
				c.logger.Errorf("Consumer processing panicked: %v\n%s", failure, failure.Stack)
			}
		}
	}
	if batchErr != nil {
		return c.blockUntilStopped(batchErr)
	}

	if err := c.r.CommitMessages(*commitMessages); err != nil {
		return c.blockUntilStopped(fmt.Errorf("commit messages: %w", err))
	}

	// Clearing resources
	*commitMessages = (*commitMessages)[:0]
	*messages = (*messages)[:0]
	return nil
}

func (c *consumer) process(message *Message) (err error) {
	stage, targetTopic := "consume", ""
	sourceTopic, partition, offset := "", 0, int64(0)
	failure := func(cause error, stack []byte) *processError {
		return &processError{
			Stage: stage, SourceTopic: sourceTopic, TargetTopic: targetTopic,
			Partition: partition, Offset: offset, Cause: cause, Stack: stack,
		}
	}
	defer func() {
		if value := recover(); value != nil {
			cause, ok := value.(error)
			if !ok {
				cause = fmt.Errorf("panic: %v", value)
			}
			err = failure(cause, debug.Stack())
		}
	}()
	if message == nil {
		return failure(errors.New("message is nil"), nil)
	}
	sourceTopic, partition, offset = message.Topic, message.Partition, message.Offset
	c.metric.observeMessageQueueLatency(message, c.consumerGroup, time.Now())
	consumeErr := c.consumeFn(message)

	if consumeErr != nil {
		if message.SendDirectToDeadLetter {
			stage, targetTopic = "dlq", c.deadLetterTopic
			if err := c.sendToDeadLetterWithBackoff(message.toDeadLetterMessage(consumeErr)); err != nil {
				return failure(err, nil)
			}

			c.metric.IncrementTotalUnprocessedMessagesCounter(1)
			return nil
		}

		if c.transactionalRetry {
			c.logger.Warnf("Consume Function Err %s, Message will be retried", consumeErr.Error())
			// Try to process same message again
			if consumeErr = c.consumeFn(message); consumeErr != nil {
				c.logger.Warnf("Consume Function Again Err %s, message is sending to exception/retry topic %s", consumeErr.Error(), c.retryTopic)
				c.metric.IncrementTotalUnprocessedMessagesCounter(1)
			}
		} else {
			c.metric.IncrementTotalUnprocessedMessagesCounter(1)
		}
	}

	if consumeErr != nil && c.retryEnabled {
		stage, targetTopic = "retry", c.retryTopic
		retryableMsg := message.toRetryableMessage(c.retryTopic, consumeErr)
		if err := c.retryWithBackoff(retryableMsg); err != nil {
			return failure(err, nil)
		}
	}

	if consumeErr == nil {
		c.metric.IncrementTotalProcessedMessagesCounter(1)
	}
	return nil
}

func (c *consumer) initializeStreams() {
	c.streamsOnce.Do(func() {
		capacity := max(c.concurrency, 1)
		c.workStream = make(chan processRequest, capacity)
		c.resultStream = make(chan processResult, capacity)
		c.stopStream = make(chan struct{})
	})
}

func (c *consumer) blockUntilStopped(err error) error {
	c.logger.Errorf("Consumer delivery blocked; source offsets will not be committed: %v", err)
	<-c.stopStream
	return err
}

func (c *consumer) Stop() error {
	c.initializeStreams()
	c.stopOnce.Do(func() { close(c.stopStream) })
	return c.base.Stop()
}
