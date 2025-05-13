package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type batchConsumer struct {
	*base

	consumeFn  BatchConsumeFn
	preBatchFn PreBatchFn

	messageGroupLimit         int
	messageGroupByteSizeLimit int
}

func (b *batchConsumer) Pause() {
	b.base.Pause()
}

func (b *batchConsumer) Resume() {
	b.base.Resume()
}

func newBatchConsumer(cfg *ConsumerConfig) (Consumer, error) {
	consumerBase, err := newBase(cfg, cfg.BatchConfiguration.MessageGroupLimit*cfg.Concurrency)
	if err != nil {
		return nil, err
	}

	messageGroupByteSizeLimit, err := resolveUnionIntOrStringValue(cfg.BatchConfiguration.MessageGroupByteSizeLimit)
	if err != nil {
		return nil, err
	}

	c := batchConsumer{
		base:                      consumerBase,
		consumeFn:                 cfg.BatchConfiguration.BatchConsumeFn,
		preBatchFn:                cfg.BatchConfiguration.PreBatchFn,
		messageGroupLimit:         cfg.BatchConfiguration.MessageGroupLimit,
		messageGroupByteSizeLimit: messageGroupByteSizeLimit,
	}

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, c.runKonsumerFn)
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (b *batchConsumer) runKonsumerFn(message kcronsumer.Message) error {
	msgList := []*Message{toMessage(message)}

	err := b.consumeFn(msgList)
	if msgList[0].ErrDescription != "" {
		err = errors.New(msgList[0].ErrDescription)
	}
	return err
}

func (b *batchConsumer) GetMetricCollectors() []prometheus.Collector {
	return b.base.GetMetricCollectors()
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

	flushTimer := time.NewTimer(b.messageGroupDuration)
	defer flushTimer.Stop()

	maximumMessageLimit := b.messageGroupLimit * b.concurrency
	maximumMessageByteSizeLimit := b.messageGroupByteSizeLimit * b.concurrency

	messages := make([]*Message, 0, maximumMessageLimit)
	commitMessages := make([]kafka.Message, 0, maximumMessageLimit)
	messageByteSize := 0

	flushBatch := func(reason string) {
		if len(messages) == 0 {
			return
		}

		b.consume(&messages, &commitMessages, &messageByteSize)

		b.logger.Debugf("[batchConsumer] Flushed batch, reason=%s", reason)

		// After flushing, we always reset the timer
		// But first we need to stop it and drain any event that might be pending
		if !flushTimer.Stop() {
			drainTimer(flushTimer)
		}

		// Now reset to start a new "rolling" interval
		flushTimer.Reset(b.messageGroupDuration)
	}

	for {
		select {
		case <-flushTimer.C:
			flushBatch("time-based (rolling timer)")
		case msg, ok := <-b.incomingMessageStream:
			if !ok {
				flushBatch("channel-closed (final flush)")
				close(b.batchConsumingStream)
				close(b.messageProcessedStream)
				return
			}

			msgSize := msg.message.TotalSize()

			// Check if there is an enough byte in batch, if not flush it.
			if maximumMessageByteSizeLimit != 0 && messageByteSize+msgSize > maximumMessageByteSizeLimit {
				flushBatch("byte-size-limit")
			}

			messages = append(messages, msg.message)
			commitMessages = append(commitMessages, *msg.kafkaMessage)
			messageByteSize += msgSize

			// Check if there is an enough size in batch, if not flush it.
			if len(messages) == maximumMessageLimit {
				flushBatch("message-count-limit")
			} else {
				// Rolling timer logic: reset the timer each time we get a new message
				// Because we "stop" it, we might need to drain the channel
				if !flushTimer.Stop() {
					drainTimer(flushTimer)
				}
				flushTimer.Reset(b.messageGroupDuration)
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

func chunkMessagesOptimized(allMessages []*Message, chunkSize int, chunkByteSize int) [][]*Message {
	if chunkSize <= 0 {
		panic("chunkSize must be greater than 0")
	}

	var chunks [][]*Message
	totalMessages := len(allMessages)
	estimatedChunks := (totalMessages + chunkSize - 1) / chunkSize
	chunks = make([][]*Message, 0, estimatedChunks)

	var currentChunk []*Message
	currentChunk = make([]*Message, 0, chunkSize)
	currentChunkBytes := 0

	for _, message := range allMessages {
		messageByteSize := len(message.Value)

		// Check if adding this message would exceed either the chunk size or the byte size
		if len(currentChunk) >= chunkSize || (chunkByteSize > 0 && currentChunkBytes+messageByteSize > chunkByteSize) {
			if len(currentChunk) == 0 {
				panic(fmt.Sprintf("invalid chunk byte size (messageGroupByteSizeLimit) %d, "+
					"message byte size is %d, bigger!, increase chunk byte size limit", chunkByteSize, messageByteSize))
			}
			chunks = append(chunks, currentChunk)
			currentChunk = make([]*Message, 0, chunkSize)
			currentChunkBytes = 0
		}

		// Add the message to the current chunk
		currentChunk = append(currentChunk, message)
		currentChunkBytes += messageByteSize
	}

	// Add the last chunk if it has any messages
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	return chunks
}

func (b *batchConsumer) consume(allMessages *[]*Message, commitMessages *[]kafka.Message, messageByteSizeLimit *int) {
	chunks := chunkMessagesOptimized(*allMessages, b.messageGroupLimit, b.messageGroupByteSizeLimit)

	if b.preBatchFn != nil {
		preBatchResult := b.preBatchFn(*allMessages)
		chunks = chunkMessagesOptimized(preBatchResult, b.messageGroupLimit, b.messageGroupByteSizeLimit)
	}

	// Send the messages to process
	for _, chunk := range chunks {
		b.batchConsumingStream <- chunk
	}

	// Wait the messages to be processed
	for i := 0; i < len(chunks); i++ {
		<-b.messageProcessedStream
	}

	if err := b.r.CommitMessages(*commitMessages); err != nil {
		b.logger.Errorf("Commit Error %s,", err.Error())
	}

	// Clearing resources
	*commitMessages = (*commitMessages)[:0]
	*allMessages = (*allMessages)[:0]
	*messageByteSizeLimit = 0
}

func countFailedMessages(chunkMessages []*Message) int64 {
	var errorCount int64
	for i := range chunkMessages {
		if chunkMessages[i].IsFailed {
			errorCount++
		}
	}
	return errorCount
}

func (b *batchConsumer) process(chunkMessages []*Message) {
	consumeErr := b.consumeFn(chunkMessages)

	if consumeErr != nil {
		if b.transactionalRetry {
			b.logger.Warnf("Consume Function Err %s, Messages will be retried", consumeErr.Error())
			// Try to process same messages again for resolving transient network errors etc.
			if consumeErr = b.consumeFn(chunkMessages); consumeErr != nil {
				b.logger.Warnf("Consume Function Again Err %s, messages are sending to exception/retry topic %s", consumeErr.Error(), b.retryTopic)
				b.metric.IncrementTotalUnprocessedMessagesCounter(int64(len(chunkMessages)))
			}
		} else {
			failedCount := countFailedMessages(chunkMessages)
			b.metric.IncrementTotalUnprocessedMessagesCounter(failedCount)
			b.metric.IncrementTotalProcessedMessagesCounter(int64(len(chunkMessages)) - failedCount)
		}

		if consumeErr != nil && b.retryEnabled {
			cronsumerMessages := make([]kcronsumer.Message, 0, len(chunkMessages))
			errorMessage := consumeErr.Error()
			if b.transactionalRetry {
				for i := range chunkMessages {
					cronsumerMessages = append(cronsumerMessages, chunkMessages[i].toRetryableMessage(b.retryTopic, errorMessage))
				}
			} else {
				for i := range chunkMessages {
					if chunkMessages[i].IsFailed {
						cronsumerMessages = append(cronsumerMessages, chunkMessages[i].toRetryableMessage(b.retryTopic, errorMessage))
					}
				}
			}

			if err := b.retryBatchWithBackoff(cronsumerMessages); err != nil {
				errorMsg := fmt.Sprintf(
					"Error producing messages to exception/retry topic: %s. Error: %s", b.retryTopic, err.Error())
				b.logger.Error(errorMsg)
				panic(errorMsg)
			}
		}
	}

	if consumeErr == nil {
		b.metric.IncrementTotalProcessedMessagesCounter(int64(len(chunkMessages)))
	}
}

func (b *batchConsumer) retryBatchWithBackoff(retryableMessages []kcronsumer.Message) error {
	var produceErr error

	for attempt := 1; attempt <= 5; attempt++ {
		produceErr = b.base.cronsumer.ProduceBatch(retryableMessages)
		if produceErr == nil {
			return nil
		}
		b.logger.Warnf("Error producing message (attempt %d/%d): %v", attempt, 5, produceErr)
		time.Sleep((50 * time.Millisecond) * time.Duration(1<<attempt))
	}

	return produceErr
}
