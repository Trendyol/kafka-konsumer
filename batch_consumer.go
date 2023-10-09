package kafka

import (
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type batchConsumer struct {
	*base

	consumeFn func(*[]Message) error

	messageGroupLimit    int
	messageGroupDuration time.Duration
	manuelRetry          bool
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
		manuelRetry:          cfg.ManuelRetryEnabled,
	}

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, func(message kcronsumer.Message) error {
			return c.consumeFn(&[]Message{toMessage(message)})
		})
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (b *batchConsumer) GetMetric() *ConsumerMetric {
	return b.metric
}

func (b *batchConsumer) Consume() {
	go b.subprocesses.Start()
	b.wg.Add(1)
	go b.startConsume()

	for i := 0; i < b.concurrency; i++ {
		b.wg.Add(1)
		go b.startBatch()
	}
}

func (b *batchConsumer) startBatch() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.messageGroupDuration)
	defer ticker.Stop()

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
	consumeErr := b.consumeFn(&messages)

	if consumeErr != nil {
		b.logger.Warnf("Consume Function Err %s, Messages will be retried", consumeErr.Error())
		// if manuel retry enabled
		if b.manuelRetry {
			b.retry(getUnsuccessfulMessages(messages))
		}
		b.retry(messages)

		if consumeErr != nil && b.retryEnabled {
			cronsumerMessages := make([]kcronsumer.Message, 0, len(messages))
			for i := range messages {
				cronsumerMessages = append(cronsumerMessages, messages[i].toRetryableMessage(b.retryTopic))
			}

			if produceErr := b.base.cronsumer.ProduceBatch(cronsumerMessages); produceErr != nil {
				b.logger.Errorf("Error producing messages to exception/retry topic %s", produceErr.Error())
			}
		}
	}

	if consumeErr == nil {
		b.metric.TotalProcessedMessagesCounter += int64(len(messages))
	}
}

func (b *batchConsumer) retry(messages []Message) {
	// Try to process same messages again
	if consumeErr := b.consumeFn(&messages); consumeErr != nil {
		b.logger.Warnf("Consume Function Again Err %s, messages are sending to exception/retry topic %s", consumeErr.Error(), b.retryTopic)
		b.metric.TotalUnprocessedMessagesCounter += int64(len(messages))
	}
	return
}

func getUnsuccessfulMessages(messages []Message) []Message {
	var unsuccessfulMessages []Message
	for _, message := range messages {
		if !message.isSuccessful {
			unsuccessfulMessages = append(unsuccessfulMessages, message)
		}
	}

	return unsuccessfulMessages
}
