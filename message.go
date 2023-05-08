package kafka

import (
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type Message kafka.Message

func (m *Message) Header(key string) *kafka.Header {
	for i, header := range m.Headers {
		if header.Key == key {
			return &m.Headers[i]
		}
	}

	return nil
}

func (m *Message) ToRetryableMessage(retryTopic string) kcronsumer.Message {
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

func ToMessage(message kcronsumer.Message) Message {
	headers := make([]protocol.Header, 0, len(message.Headers))
	for i := range message.Headers {
		headers = append(headers, protocol.Header{
			Key:   message.Headers[i].Key,
			Value: message.Headers[i].Value,
		})
	}

	return Message{
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

func (m *Message) AddHeader(header ...kafka.Header) {
	m.Headers = append(m.Headers, header...)
}

func (m *Message) RemoveHeader(header kafka.Header) {
	for i, h := range m.Headers {
		if h.Key == header.Key {
			m.Headers = append(m.Headers[:i], m.Headers[i+1:]...)
			break
		}
	}
}
