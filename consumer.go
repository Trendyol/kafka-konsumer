package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type consumer struct {
	*base

	consumeFn func(*Message) error
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

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, func(message kcronsumer.Message) error {
			return c.consumeFn(toMessage(message))
		})
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (c *consumer) Consume() {
	go c.subprocesses.Start()
	c.wg.Add(1)
	go c.startConsume()

	c.wg.Add(1)
	go c.startBatch()
}

func (c *consumer) startBatch() {
	defer c.wg.Done()

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for message := range c.singleMessageCommitCh {
				c.process(message)
				c.waitMessageProcess <- struct{}{}
			}
		}()
	}

	ticker := time.NewTicker(c.messageGroupDuration)
	defer ticker.Stop()

	messages := make([]*Message, 0, c.concurrency)
	commitMessages := make([]kafka.Message, 0, c.concurrency)

	for {
		select {
		case <-ticker.C:
			if len(messages) == 0 {
				continue
			}

			c.consume(messages, &commitMessages)
			messages = messages[:0]
		case msg, ok := <-c.messageCh:
			if !ok {
				return
			}

			messages = append(messages, msg)

			if len(messages) == c.concurrency {
				c.consume(messages, &commitMessages)
				messages = messages[:0]
			}
		}
	}
}

func (c *consumer) consume(messages []*Message, commitMessages *[]kafka.Message) {
	for _, message := range messages {
		c.singleMessageCommitCh <- message
	}

	for i := 0; i < len(messages); i++ {
		<-c.waitMessageProcess
	}

	toKafkaMessages(messages, commitMessages)
	err := c.r.CommitMessages(*commitMessages)
	*commitMessages = (*commitMessages)[:0]
	if err != nil {
		c.logger.Errorf("Commit Error %s,", err.Error())
	}
}

func (c *consumer) process(message *Message) {
	consumeErr := c.consumeFn(message)

	if consumeErr != nil {
		c.logger.Warnf("Consume Function Err %s, Message will be retried", consumeErr.Error())
		// Try to process same message again
		if consumeErr = c.consumeFn(message); consumeErr != nil {
			c.logger.Warnf("Consume Function Again Err %s, message is sending to exception/retry topic %s", consumeErr.Error(), c.retryTopic)
			c.metric.TotalUnprocessedMessagesCounter++
		}
	}

	if consumeErr != nil && c.retryEnabled {
		retryableMsg := message.toRetryableMessage(c.retryTopic)
		if produceErr := c.cronsumer.Produce(retryableMsg); produceErr != nil {
			c.logger.Errorf("Error producing message %s to exception/retry topic %s",
				string(retryableMsg.Value), produceErr.Error())
		}
	}

	if consumeErr == nil {
		c.metric.TotalProcessedMessagesCounter++
	}
}
