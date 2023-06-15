package kafka

import (
	"context"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-konsumer/instrumentation"
	"github.com/segmentio/kafka-go"
)

type consumer struct {
	*base

	consumeFn func(Message) error
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
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
		c.base.setupAPI(cfg)
	}

	return &c, nil
}

func (c *consumer) Consume() {
	go c.subprocesses.Start()
	go c.base.startConsume()

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

func (c *consumer) process(message Message) {
	consumeErr := c.consumeFn(message)

	if consumeErr != nil && c.retryEnabled {
		c.logger.Warnf("Consume Function Err %s, Message will be retried", consumeErr.Error())

		// Try to process same message again
		if consumeErr = c.consumeFn(message); consumeErr != nil {
			c.logger.Warnf("Consume Function Again Err %s, message is sending to exception/retry topic %s", consumeErr.Error(), c.retryTopic)

			retryableMsg := message.toRetryableMessage(c.retryTopic)
			if produceErr := c.cronsumer.Produce(retryableMsg); produceErr != nil {
				c.logger.Errorf("Error producing message %s to exception/retry topic %s",
					string(retryableMsg.Value), produceErr.Error())
			}
		}
	}

	commitErr := c.r.CommitMessages(context.Background(), kafka.Message(message))
	if commitErr != nil {
		instrumentation.TotalUnprocessedMessagesCounter.Inc()
		c.logger.Errorf("Error Committing message %s, %s", string(message.Value), commitErr.Error())
		return
	}

	instrumentation.TotalProcessedMessagesCounter.Inc()
}
