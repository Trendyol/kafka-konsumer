package kafka

import (
	"context"
	"sync"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type Header = protocol.Header

type Message struct {
	Time       time.Time
	WriterData interface{}

	// Context To enable distributed tracing support
	Context       context.Context
	Topic         string
	Key           []byte
	Value         []byte
	Headers       []Header
	Partition     int
	Offset        int64
	HighWaterMark int64

	// IsFailed Is only used on transactional retry disabled
	IsFailed bool
}

func (m *Message) toKafkaMessage() kafka.Message {
	return kafka.Message{
		Topic:         m.Topic,
		Partition:     m.Partition,
		Offset:        m.Offset,
		HighWaterMark: m.HighWaterMark,
		Key:           m.Key,
		Value:         m.Value,
		Headers:       m.Headers,
		WriterData:    m.WriterData,
		Time:          m.Time,
	}
}

func toKafkaMessages(messages *[]*Message, commitMessages *[]kafka.Message) {
	for _, message := range *messages {
		*commitMessages = append(*commitMessages, message.toKafkaMessage())
	}
}

func putMessages(messages *[]*Message) {
	for _, message := range *messages {
		messagePool.Put(message)
	}
}

var messagePool = sync.Pool{
	New: func() any {
		return &Message{}
	},
}

func fromKafkaMessage(kafkaMessage *kafka.Message) *Message {
	message := messagePool.Get().(*Message)

	message.Topic = kafkaMessage.Topic
	message.Partition = kafkaMessage.Partition
	message.Offset = kafkaMessage.Offset
	message.HighWaterMark = kafkaMessage.HighWaterMark
	message.Key = kafkaMessage.Key
	message.Value = kafkaMessage.Value
	message.Headers = kafkaMessage.Headers
	message.WriterData = kafkaMessage.WriterData
	message.Time = kafkaMessage.Time
	message.Context = context.TODO()

	return message
}

func (m *Message) toRetryableMessage(retryTopic string) kcronsumer.Message {
	headers := make([]kcronsumer.Header, 0, len(m.Headers))
	for i := range m.Headers {
		headers = append(headers, kcronsumer.Header{
			Key:   m.Headers[i].Key,
			Value: m.Headers[i].Value,
		})
	}

	return kcronsumer.NewMessageBuilder().
		WithKey(m.Key).
		WithValue(m.Value).
		WithTopic(retryTopic).
		WithHeaders(headers).
		WithPartition(m.Partition).
		WithHighWatermark(m.HighWaterMark).
		Build()
}

func toMessage(message kcronsumer.Message) *Message {
	headers := make([]protocol.Header, 0, len(message.Headers))
	for i := range message.Headers {
		headers = append(headers, protocol.Header{
			Key:   message.Headers[i].Key,
			Value: message.Headers[i].Value,
		})
	}

	msg := messagePool.Get().(*Message)
	msg.Topic = message.Topic
	msg.Partition = message.Partition
	msg.Offset = message.Offset
	msg.HighWaterMark = message.HighWaterMark
	msg.Key = message.Key
	msg.Value = message.Value
	msg.Headers = headers
	msg.Time = message.Time

	return msg
}

func (m *Message) Header(key string) *kafka.Header {
	for i, header := range m.Headers {
		if header.Key == key {
			return &m.Headers[i]
		}
	}

	return nil
}

// AddHeader works as a idempotent function
func (m *Message) AddHeader(header Header) {
	for i := range m.Headers {
		if m.Headers[i].Key == header.Key {
			m.Headers[i].Value = header.Value
			return
		}
	}

	m.Headers = append(m.Headers, header)
}

func (m *Message) RemoveHeader(header Header) {
	for i, h := range m.Headers {
		if h.Key == header.Key {
			m.Headers = append(m.Headers[:i], m.Headers[i+1:]...)
			break
		}
	}
}
