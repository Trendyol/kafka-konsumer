package kafka

import (
	"context"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type Header = protocol.Header

type Message struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64

	// IsFailed Is only used on transactional retry disabled
	IsFailed bool

	Key        []byte
	Value      []byte
	Headers    []Header
	WriterData interface{}
	Time       time.Time

	// Context To enable distributed tracing support
	Context context.Context
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

func fromKafkaMessage(message *kafka.Message) *Message {
	return &Message{
		Topic:         message.Topic,
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Headers:       message.Headers,
		WriterData:    message.WriterData,
		Time:          message.Time,
		Context:       context.TODO(),
	}
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

	return &Message{
		Topic:         message.Topic,
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Headers:       headers,
		Time:          message.Time,
	}
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
