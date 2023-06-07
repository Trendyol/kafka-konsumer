package kafka

import (
	"github.com/segmentio/kafka-go"
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
