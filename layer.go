package kafka

import (
	"crypto/tls"
	"github.com/segmentio/kafka-go/sasl"
)

type Layer interface {
	SetTLSConfig(config *tls.Config)
	SetSASL(mechanism sasl.Mechanism)
}

func fillLayer(layer Layer, sasl *SASLConfig, tls *TLSConfig) error {
	if sasl != nil {
		mechanism, err := sasl.Mechanism()
		if err != nil {
			return err
		}

		layer.SetSASL(mechanism)
	}

	if tls != nil {
		config, err := tls.TLSConfig()
		if err != nil {
			return err
		}

		layer.SetTLSConfig(config)
	}

	return nil
}
