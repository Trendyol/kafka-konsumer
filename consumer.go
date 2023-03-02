package kafka

import (
	"context"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"sync"
)

type Consumer interface {
	Consume(func(Message))
	Stop() error
	Lag() int64
}

type consumer struct {
	r           *kafka.Reader
	m           sync.Mutex
	wg          sync.WaitGroup
	once        sync.Once
	messageCh   chan Message // TODO: enhancement sync pool yapılabilir mi
	quit        chan struct{}
	concurrency int
	cronsumer   kcronsumer.Cronsumer
}

var _ Consumer = (*consumer)(nil)

func NewConsumer(cfg ConsumerConfig) (Consumer, error) {
	c := consumer{
		messageCh:   make(chan Message, cfg.Concurrency),
		quit:        make(chan struct{}),
		concurrency: cfg.Concurrency,
	}

	var err error

	c.r, err = cfg.newKafkaReader()
	if err != nil {
		return nil, err
	}

	if cfg.CronsumerEnabled {
		c.cronsumer = cronsumer.New(cfg.CronsumerConfig, cfg.ExceptionFunc)
	}

	return &c, nil
}

func (c *consumer) Consume(processFn func(Message)) {
	c.cronsumer.Start()

	go c.consume()

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for message := range c.messageCh {
				processFn(message)
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
				continue
			}

			c.messageCh <- Message(message)
		}
	}
}

func (c *consumer) Stop() error {
	var err error
	c.once.Do(func() {
		c.cronsumer.Stop()
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
