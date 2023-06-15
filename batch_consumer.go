package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

type batchConsumer struct {
	r           *kafka.Reader
	wg          sync.WaitGroup
	once        sync.Once
	messageCh   chan Message
	quit        chan struct{}
	concurrency int
	consumeFn   func([]Message) error

	messageGroupLimit    int
	messageGroupDuration time.Duration

	cancelFn context.CancelFunc
	context  context.Context

	logger LoggerInterface
}

var _ Consumer = (*batchConsumer)(nil)

func NewBatchConsumer(cfg *ConsumerConfig) (Consumer, error) {
	log := NewZapLogger(cfg.LogLevel)
	reader, err := cfg.newKafkaReader()
	if err != nil {
		log.Errorf("Error when initializing kafka reader %v", err)
		return nil, err
	}

	c := batchConsumer{
		r:                    reader,
		messageCh:            make(chan Message, cfg.Concurrency),
		quit:                 make(chan struct{}),
		concurrency:          cfg.Concurrency,
		consumeFn:            cfg.BatchConfiguration.BatchConsumeFn,
		messageGroupLimit:    cfg.BatchConfiguration.MessageGroupLimit,
		messageGroupDuration: cfg.BatchConfiguration.MessageGroupDuration,
		logger:               log,
	}

	c.context, c.cancelFn = context.WithCancel(context.Background())

	return &c, nil
}

func (b *batchConsumer) Consume() {
	go b.consume()

	for i := 0; i < b.concurrency; i++ {
		b.wg.Add(1)
		go b.startBatch()
	}
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

			b.processMessage(messages)
			messages = messages[:0]
		case msg, ok := <-b.messageCh:
			if !ok {
				return
			}

			messages = append(messages, msg)

			if len(messages) == b.messageGroupLimit {
				b.processMessage(messages)
				messages = messages[:0]
			}
		}
	}
}

func (b *batchConsumer) processMessage(messages []Message) {
	if err := b.consumeFn(messages); err != nil {
		return
	}

	segmentioMessages := make([]kafka.Message, 0, len(messages))
	for i := range messages {
		segmentioMessages = append(segmentioMessages, kafka.Message(messages[i]))
	}

	commitErr := b.r.CommitMessages(context.Background(), segmentioMessages...)
	if commitErr != nil {
		b.logger.Error("Error Committing messages %s", commitErr.Error())
		return
	}
}

func (b *batchConsumer) consume() {
	b.wg.Add(1)
	defer b.wg.Done()

	for {
		select {
		case <-b.quit:
			return
		default:
			message, err := b.r.FetchMessage(b.context)
			if err != nil {
				if b.context.Err() != nil {
					continue
				}
				b.logger.Errorf("Message could not read, err %s", err.Error())
				continue
			}

			b.messageCh <- Message(message)
		}
	}
}

func (b *batchConsumer) WithLogger(logger LoggerInterface) {
	b.logger = logger
}

func (b *batchConsumer) Stop() error {
	b.logger.Debug("Consuming is closing!")
	var err error
	b.once.Do(func() {
		b.cancelFn()
		b.quit <- struct{}{}
		close(b.messageCh)
		b.wg.Wait()
		err = b.r.Close()
	})

	return err
}
