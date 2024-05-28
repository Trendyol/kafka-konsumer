package kafka

import (
	"context"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

const (
	errMessageKey = "x-error-message"
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

	// ErrDescription specifies the IsFailed message's error

	// If available, kafka-konsumer writes this description into the failed message's
	// headers as `x-error-message` key when producing retry topic
	ErrDescription string
}

func (msg *Message) TotalSize() int {
	return 14 + msg.keySize() + msg.valueSize() + msg.headerSize()
}

func (msg *Message) headerSize() int {
	s := 0
	for _, header := range msg.Headers {
		s += sizeofString(header.Key)
		s += len(header.Value)
	}
	return s
}

func (msg *Message) keySize() int {
	return sizeofBytes(msg.Key)
}

func (msg *Message) valueSize() int {
	return sizeofBytes(msg.Value)
}

type IncomingMessage struct {
	kafkaMessage *kafka.Message
	message      *Message
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

func fromKafkaMessage(kafkaMessage *kafka.Message) *Message {
	message := &Message{}
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

func (m *Message) toRetryableMessage(retryTopic, consumeError string) kcronsumer.Message {
	headers := make([]kcronsumer.Header, 0, len(m.Headers))
	for i := range m.Headers {
		headers = append(headers, kcronsumer.Header{
			Key:   m.Headers[i].Key,
			Value: m.Headers[i].Value,
		})
	}

	if m.ErrDescription == "" {
		headers = append(headers, kcronsumer.Header{
			Key:   errMessageKey,
			Value: []byte(consumeError),
		})
	} else {
		headers = append(headers, kcronsumer.Header{
			Key:   errMessageKey,
			Value: []byte(m.ErrDescription),
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

	msg := &Message{}
	msg.Topic = message.Topic
	msg.Partition = message.Partition
	msg.Offset = message.Offset
	msg.HighWaterMark = message.HighWaterMark
	msg.Key = message.Key
	msg.Value = message.Value
	msg.Headers = headers
	msg.Time = message.Time
	msg.Context = context.Background()

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

func sizeofBytes(b []byte) int {
	return 4 + len(b)
}

func sizeofString(s string) int {
	return 2 + len(s)
}
