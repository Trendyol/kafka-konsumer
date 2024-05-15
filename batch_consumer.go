package kafka

import (
	"errors"
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

	messageGroupByteSizeLimit, err := ResolveUnionIntOrStringValue(cfg.BatchConfiguration.MessageGroupByteSizeLimit)
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

	ticker := time.NewTicker(b.messageGroupDuration)
	defer ticker.Stop()

	maximumMessageLimit := b.messageGroupLimit * b.concurrency
	maximumMessageByteSizeLimit := b.messageGroupByteSizeLimit * b.concurrency
	messages := make([]*Message, 0, maximumMessageLimit)
	commitMessages := make([]kafka.Message, 0, maximumMessageLimit)
	messageByteSize := 0
	for {
		select {
		case <-ticker.C:
			if len(messages) == 0 {
				continue
			}

			b.consume(&messages, &commitMessages, &messageByteSize)
		case msg, ok := <-b.incomingMessageStream:
			if !ok {
				close(b.batchConsumingStream)
				close(b.messageProcessedStream)
				return
			}

			if maximumMessageByteSizeLimit != 0 && messageByteSize+len(msg.message.Value) > maximumMessageByteSizeLimit {
				b.consume(&messages, &commitMessages, &messageByteSize)
			}

			messages = append(messages, msg.message)
			commitMessages = append(commitMessages, *msg.kafkaMessage)
			messageByteSize += len(msg.message.Value)

			if len(messages) == maximumMessageLimit {
				b.consume(&messages, &commitMessages, &messageByteSize)
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

func chunkMessages(allMessages *[]*Message, chunkSize int, chunkByteSize int) [][]*Message {
	var chunks [][]*Message

	allMessageList := *allMessages
	var currentChunk []*Message
	currentChunkSize := 0
	currentChunkBytes := 0

	for _, message := range allMessageList {
		messageByteSize := len(message.Value)

		// Check if adding this message would exceed either the chunk size or the byte size
		if len(currentChunk) >= chunkSize || (chunkByteSize != 0 && currentChunkBytes+messageByteSize > chunkByteSize) {
			// Avoid too low chunkByteSize
			if len(currentChunk) == 0 {
				panic("invalid chunk byte size, please increase it")
			}
			// If it does, finalize the current chunk and start a new one
			chunks = append(chunks, currentChunk)
			currentChunk = []*Message{}
			currentChunkSize = 0
			currentChunkBytes = 0
		}

		// Add the message to the current chunk
		currentChunk = append(currentChunk, message)
		currentChunkSize++
		currentChunkBytes += messageByteSize
	}

	// Add the last chunk if it has any messages
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	return chunks
}

func (b *batchConsumer) consume(allMessages *[]*Message, commitMessages *[]kafka.Message, messageByteSizeLimit *int) {
	chunks := chunkMessages(allMessages, b.messageGroupLimit, b.messageGroupByteSizeLimit)

	if b.preBatchFn != nil {
		preBatchResult := b.preBatchFn(*allMessages)
		chunks = chunkMessages(&preBatchResult, b.messageGroupLimit, b.messageGroupByteSizeLimit)
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

			if produceErr := b.base.cronsumer.ProduceBatch(cronsumerMessages); produceErr != nil {
				b.logger.Errorf("Error producing messages to exception/retry topic %s", produceErr.Error())
			}
		}
	}

	if consumeErr == nil {
		b.metric.TotalProcessedMessagesCounter += int64(len(chunkMessages))
	}
}
