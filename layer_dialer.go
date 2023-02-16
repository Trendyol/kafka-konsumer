package kafka

import (
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

type Dialer struct {
	*kafka.Dialer
}

func newDialer() *Dialer {
	return &Dialer{
		Dialer: &kafka.Dialer{},
	}
}

func (t *Dialer) SetTLSConfig(config *tls.Config) {
	t.TLS = config
}

func (t *Dialer) SetSASL(mechanism sasl.Mechanism) {
	t.SASLMechanism = mechanism
}
