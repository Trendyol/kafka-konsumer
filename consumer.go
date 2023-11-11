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
	maxAttempt := 3

	for attempt := 1; attempt <= maxAttempt; attempt++ {
		consumeErr = c.consumeFn(message)

		if consumeErr == nil {
			break
		}
		c.logger.Warnf("Consume Function Err: %s, at attempt %d", consumeErr.Error(), attempt)

		if attempt != maxAttempt {
			time.Sleep(exponentialBackoff)
			exponentialBackoff *= 2
		}
	}

	if consumeErr != nil {
		c.metric.TotalUnprocessedMessagesCounter++
		if c.retryEnabled {
			c.logger.Warnf("Consume Function Again Err: %s, after %d attempts, message is sending to exception/retry topic %s", consumeErr.Error(), maxAttempt, c.retryTopic)
			retryableMsg := message.toRetryableMessage(c.retryTopic)
			if produceErr := c.cronsumer.Produce(retryableMsg); produceErr != nil {
				c.logger.Errorf("Error producing message %s to exception/retry topic %s",
					string(retryableMsg.Value), produceErr.Error())
			}
		}
		return
	}
	c.metric.TotalProcessedMessagesCounter++
}
