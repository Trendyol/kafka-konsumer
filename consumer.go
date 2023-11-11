package kafka

import (
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type consumer struct {
	*base

	consumeFn func(*Message) error
}

func newSingleConsumer(cfg *ConsumerConfig) (Consumer, error) {
	consumerBase, err := newBase(cfg)
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

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for message := range c.messageCh {
				c.process(message)
			}
		}()
	}
}

func (c *consumer) process(message *Message) {
	var consumeErr error

	exponentialBackoff := time.Second
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		consumeErr = c.consumeFn(message)

		if consumeErr == nil {
			break
		}

		c.logger.Warnf("Consume Function Err %s, Message will be retried at attempt %d", consumeErr.Error(), attempt)

		time.Sleep(exponentialBackoff)
		exponentialBackoff *= 2
	}

	if consumeErr != nil {
		c.metric.TotalUnprocessedMessagesCounter++
		if c.retryEnabled {
			c.logger.Warnf("Consume Function Again Err %s after %d tries, message is sending to exception/retry topic %s", consumeErr.Error(), maxRetries, c.retryTopic)
			retryableMsg := message.toRetryableMessage(c.retryTopic)
			if produceErr := c.cronsumer.Produce(retryableMsg); produceErr != nil {
				c.logger.Errorf("Error producing message %s to exception/retry topic %s",
					string(retryableMsg.Value), produceErr.Error())
			}
		}
	} else {
		c.metric.TotalProcessedMessagesCounter++
	}
}
