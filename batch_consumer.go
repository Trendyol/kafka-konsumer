package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type batchConsumer struct {
	*base

	consumeFn  BatchConsumeFn
	preBatchFn PreBatchFn

	messageGroupLimit int
}

func newBatchConsumer(cfg *ConsumerConfig) (Consumer, error) {
	consumerBase, err := newBase(cfg, cfg.BatchConfiguration.MessageGroupLimit*cfg.Concurrency)
	if err != nil {
		return nil, err
	}

	c := batchConsumer{
		base:              consumerBase,
		consumeFn:         cfg.BatchConfiguration.BatchConsumeFn,
		preBatchFn:        cfg.BatchConfiguration.PreBatchFn,
		messageGroupLimit: cfg.BatchConfiguration.MessageGroupLimit,
	}

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, func(message kcronsumer.Message) error {
			return c.consumeFn([]*Message{toMessage(message)})
		})
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (b *batchConsumer) GetMetricCollectors() []prometheus.Collector {
	return b.base.GetMetricCollectors()
}

func (b *batchConsumer) GetMetric() *ConsumerMetric {
	return b.metric
}

func (b *batchConsumer) Consume() {
	go b.subprocesses.Start()

	b.wg.Add(1)
	go b.startConsume()

	b.wg.Add(b.concurrency)
	b.setupConcurrentWorkers()

	b.wg.Add(1)
	go b.startBatch()
}

func (b *batchConsumer) startBatch() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.messageGroupDuration)
	defer ticker.Stop()

	maximumMessageLimit := b.messageGroupLimit * b.concurrency
	messages := make([]*Message, 0, maximumMessageLimit)
	commitMessages := make([]kafka.Message, 0, maximumMessageLimit)

	for {
		select {
		case <-ticker.C:
			if len(messages) == 0 {
				continue
			}

			b.consume(&messages, &commitMessages)
		case msg, ok := <-b.incomingMessageStream:
			if !ok {
				close(b.batchConsumingStream)
				close(b.messageProcessedStream)
				return
			}

			messages = append(messages, msg)

			if len(messages) == maximumMessageLimit {
				b.consume(&messages, &commitMessages)
			}
		}
	}
}

func (b *batchConsumer) setupConcurrentWorkers() {
	for i := 0; i < b.concurrency; i++ {
		go func() {
			defer b.wg.Done()
			for messages := range b.batchConsumingStream {
				b.process(messages)
				b.messageProcessedStream <- struct{}{}
			}
		}()
	}
}

func chunkMessages(allMessages *[]*Message, chunkSize int) [][]*Message {
	var chunks [][]*Message

	allMessageList := *allMessages
	for i := 0; i < len(allMessageList); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(allMessageList) {
			end = len(allMessageList)
		}

		chunks = append(chunks, allMessageList[i:end])
	}

	return chunks
}

func (b *batchConsumer) consume(allMessages *[]*Message, commitMessages *[]kafka.Message) {
	chunks := chunkMessages(allMessages, b.messageGroupLimit)

	if b.preBatchFn != nil {
		preBatchResult := b.preBatchFn(*allMessages)
		chunks = chunkMessages(&preBatchResult, b.messageGroupLimit)
	}

	// Send the messages to process
	for _, chunk := range chunks {
		b.batchConsumingStream <- chunk
	}

	// Wait the messages to be processed
	for i := 0; i < len(chunks); i++ {
		<-b.messageProcessedStream
	}

	toKafkaMessages(allMessages, commitMessages)
	if err := b.r.CommitMessages(*commitMessages); err != nil {
		b.logger.Errorf("Commit Error %s,", err.Error())
	}

	// Clearing resources
	putKafkaMessage(commitMessages)
	putMessages(allMessages)
	*commitMessages = (*commitMessages)[:0]
	*allMessages = (*allMessages)[:0]
}

func (b *batchConsumer) process(chunkMessages []*Message) {
	consumeErr := b.consumeFn(chunkMessages)

	if consumeErr != nil {
		b.logger.Warnf("Consume Function Err %s, Messages will be retried", consumeErr.Error())
		// Try to process same messages again for resolving transient network errors etc.
		if consumeErr = b.consumeFn(chunkMessages); consumeErr != nil {
			b.logger.Warnf("Consume Function Again Err %s, messages are sending to exception/retry topic %s", consumeErr.Error(), b.retryTopic)
			b.metric.TotalUnprocessedMessagesCounter += int64(len(chunkMessages))
		}

		if consumeErr != nil && b.retryEnabled {
			cronsumerMessages := make([]kcronsumer.Message, 0, len(chunkMessages))
			if b.transactionalRetry {
				for i := range chunkMessages {
					cronsumerMessages = append(cronsumerMessages, chunkMessages[i].toRetryableMessage(b.retryTopic))
				}
			} else {
				for i := range chunkMessages {
					if chunkMessages[i].IsFailed {
						cronsumerMessages = append(cronsumerMessages, chunkMessages[i].toRetryableMessage(b.retryTopic))
					}
				}
			}

			if produceErr := b.base.cronsumer.ProduceBatch(cronsumerMessages); produceErr != nil {
				b.logger.Errorf("Error producing messages to exception/retry topic %s", produceErr.Error())
			}
		}
	}

	if consumeErr == nil {
		b.metric.TotalProcessedMessagesCounter += int64(len(chunkMessages))
	}
}
