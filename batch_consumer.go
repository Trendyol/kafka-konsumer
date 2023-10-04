package kafka

import (
	"context"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

type batchConsumer struct {
	*base

	consumeFn func([]Message) error

	messageGroupLimit    int
	messageGroupDuration time.Duration
}

func newBatchConsumer(cfg *ConsumerConfig) (Consumer, error) {
	consumerBase, err := newBase(cfg)
	if err != nil {
		return nil, err
	}

	c := batchConsumer{
		base:                 consumerBase,
		consumeFn:            cfg.BatchConfiguration.BatchConsumeFn,
		messageGroupLimit:    cfg.BatchConfiguration.MessageGroupLimit,
		messageGroupDuration: cfg.BatchConfiguration.MessageGroupDuration,
	}

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, func(message kcronsumer.Message) error {
			return c.consumeFn([]Message{toMessage(message)})
		})
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric, c.base.cronsumer.GetMetricCollectors()...)
	}

	consumerBase.retryFunc = c.retry

	return &c, nil
}

func (b *batchConsumer) Consume() {
	go b.base.subprocesses.Start()
	go b.startConsume()

	for i := 0; i < b.concurrency; i++ {
		b.wg.Add(1)
		go b.startBatch()
	}
}

func (b *batchConsumer) GetMetric() *ConsumerMetric {
	return b.metric
}

func (b *batchConsumer) startBatch() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.messageGroupDuration)
	messages := make([]Message, 0, b.messageGroupLimit)

	for {
		select {
		case <-ticker.C:
			if len(messages) == 0 {
				continue
			}

			b.process(messages)
			messages = messages[:0]
		case msg, ok := <-b.messageCh:
			if !ok {
				return
			}

			messages = append(messages, msg)

			if len(messages) == b.messageGroupLimit {
				b.process(messages)
				messages = messages[:0]
			}
		}
	}
}

func (b *batchConsumer) process(messages []Message) {
	consumeErr := b.consumeFn(messages)
	if consumeErr != nil && b.retryEnabled && !b.manuelRetryEnabled {
		b.retry(messages)
	}

	segmentioMessages := make([]kafka.Message, 0, len(messages))
	for i := range messages {
		segmentioMessages = append(segmentioMessages, kafka.Message(messages[i]))
	}

	commitErr := b.r.CommitMessages(context.Background(), segmentioMessages...)
	if commitErr != nil {
		b.metric.TotalUnprocessedBatchMessagesCounter++
		b.logger.Error("Error Committing messages %s", commitErr.Error())
		return
	}

	b.metric.TotalProcessedBatchMessagesCounter++
}

func (b *batchConsumer) retry(messages []Message) {
	b.logger.Warnf("Messages will be retried")

	// Try to process same message again
	if consumeErr := b.consumeFn(messages); consumeErr != nil {
		b.logger.Warnf("Consume Function Again Err %s, messages are sending to exception/retry topic %s", consumeErr.Error(), b.retryTopic)

		cronsumerMessages := make([]kcronsumer.Message, 0, len(messages))
		for i := range messages {
			cronsumerMessages = append(cronsumerMessages, messages[i].toRetryableMessage(b.retryTopic))
		}

		if produceErr := b.base.cronsumer.ProduceBatch(cronsumerMessages); produceErr != nil {
			b.logger.Errorf("Error producing messages to exception/retry topic %s", produceErr.Error())
		}
	}

	return
}
