package kafka

import (
	"crypto/tls"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

type Transport struct {
	*kafka.Transport
}

func newTransport() *Transport {
	return &Transport{
		Transport: &kafka.Transport{},
	}
}

func (t *Transport) SetTLSConfig(config *tls.Config) {
	t.TLS = config
}

func (t *Transport) SetSASL(mechanism sasl.Mechanism) {
	t.SASL = mechanism
}
