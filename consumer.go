package kafka

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type consumer struct {
	*base

	consumeFn func(*Message) error
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

	if cfg.RetryEnabled || (cfg.DeadLetterTopic != "") {
		c.base.setupCronsumer(cfg, func(message kcronsumer.Message) error {
			return c.consumeFn(toMessage(message))
		})
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (c *consumer) GetMetricCollectors() []prometheus.Collector {
	return c.base.GetMetricCollectors()
}

func (c *consumer) Consume() {
	go c.subprocesses.Start()

	c.wg.Add(1)
	go c.startConsume()

	c.setupConcurrentWorkers()

	c.wg.Add(1)
	go c.startBatch()
}

func (c *consumer) startBatch() {
	defer c.wg.Done()

	flushTimer := time.NewTimer(c.messageGroupDuration)
	defer flushTimer.Stop()

	messages := make([]*Message, 0, c.concurrency)
	commitMessages := make([]kafka.Message, 0, c.concurrency)

	flushBatch := func(reason string) {
		if len(messages) == 0 {
			return
		}

		c.consume(&messages, &commitMessages)

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
				close(c.singleConsumingStream)
				close(c.messageProcessedStream)
				return
			}

			messages = append(messages, msg.message)
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
	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for message := range c.singleConsumingStream {
				c.process(message)
				c.messageProcessedStream <- struct{}{}
			}
		}()
	}
}

func (c *consumer) consume(messages *[]*Message, commitMessages *[]kafka.Message) {
	messageList := *messages

	// Send the messages to process
	for _, message := range messageList {
		c.singleConsumingStream <- message
	}

	// Wait the messages to be processed
	for range messageList {
		<-c.messageProcessedStream
	}

	if err := c.r.CommitMessages(*commitMessages); err != nil {
		c.logger.Errorf("Commit Error %s,", err.Error())
	}

	// Clearing resources
	*commitMessages = (*commitMessages)[:0]
	*messages = (*messages)[:0]
}

func (c *consumer) process(message *Message) {
	consumeErr := c.consumeFn(message)

	if consumeErr != nil {
		if message.SendDirectToDeadLetter {
			deadLetterTopic := c.deadLetterTopic
			if deadLetterTopic == "" && c.consumerCfg != nil && c.consumerCfg.RetryConfiguration.DeadLetterTopic != "" {
				deadLetterTopic = c.consumerCfg.RetryConfiguration.DeadLetterTopic
			}

			if deadLetterTopic != "" && c.cronsumer != nil {
				c.logger.Warnf("Message with error is being sent directly to dead letter topic: %s", deadLetterTopic)
				retryableMsg := message.toRetryableMessage(deadLetterTopic, consumeErr.Error())
				if err := c.retryWithBackoff(retryableMsg); err != nil {
					errorMessage := fmt.Sprintf(
						"Error producing message %s to dead letter topic %s. Error: %s",
						string(message.Value), deadLetterTopic, err.Error())
					c.logger.Error(errorMessage)
					panic(err.Error())
				}
				c.metric.IncrementTotalUnprocessedMessagesCounter(1)
			} else if deadLetterTopic == "" {
				c.logger.Warn("SendDirectToDeadLetter is true but no dead letter topic is configured")
			} else if c.cronsumer == nil {
				c.logger.Warn("SendDirectToDeadLetter is true but cronsumer is not initialized. " +
					"Make sure RetryEnabled is true or RetryConfiguration.DeadLetterTopic is configured")
			}
			return
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
		retryableMsg := message.toRetryableMessage(c.retryTopic, consumeErr.Error())
		if err := c.retryWithBackoff(retryableMsg); err != nil {
			errorMessage := fmt.Sprintf(
				"Error producing message %s to exception/retry topic %s. Error: %s",
				string(message.Value), c.retryTopic, err.Error())
			c.logger.Error(errorMessage)
			panic(err.Error())
		}
	}

	if consumeErr == nil {
		c.metric.IncrementTotalProcessedMessagesCounter(1)
	}
}

func (c *consumer) retryWithBackoff(retryableMsg kcronsumer.Message) error {
	var produceErr error

	for attempt := 1; attempt <= 5; attempt++ {
		produceErr = c.cronsumer.Produce(retryableMsg)
		if produceErr == nil {
			return nil
		}
		c.logger.Warnf("Error producing message (attempt %d/%d): %v", attempt, 5, produceErr)
		time.Sleep((50 * time.Millisecond) * time.Duration(1<<attempt))
	}

	return produceErr
}
