package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"io"
	"sync"
)

type Processor interface {
	Process(message Message)
}

type Consumer interface {
	Consume(processor Processor)
	Stop() error
	Lag() int64
}

type consumer struct {
	r           *kafka.Reader
	m           sync.Mutex
	wg          sync.WaitGroup
	once        sync.Once
	messageCh   chan Message
	quit        chan struct{}
	concurrency int
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg ConsumerConfig) (Consumer, error) {
	reader, err := cfg.newKafkaReader()
	if err != nil {
		return nil, err
	}

	return &consumer{
		r:           reader,
		messageCh:   make(chan Message, cfg.Concurrency),
		quit:        make(chan struct{}),
		concurrency: cfg.Concurrency,
	}, nil
}

func (c *consumer) Consume(processor Processor) {
	go c.consume()

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for message := range c.messageCh {
				processor.Process(message)
			}
		}()
	}
}

func (c *consumer) consume() {
	c.wg.Add(1)
	defer c.wg.Done()
	for {
		select {
		case <-c.quit:
			return
		default:
			message, err := c.r.ReadMessage(context.Background())
			if err != nil {
				if err == io.EOF {
					break
				}

				continue
			}

			c.messageCh <- Message(message)
		}
	}
}

func (c *consumer) Stop() error {
	var err error
	c.once.Do(func() {
		err = c.r.Close()
		c.quit <- struct{}{}
		close(c.messageCh)
		c.wg.Wait()
	})

	return err
}

func (c *consumer) Lag() int64 {
	return c.r.Lag()
}
