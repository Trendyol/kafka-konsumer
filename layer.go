package kafka

import (
	"crypto/tls"
	"github.com/segmentio/kafka-go/sasl"
)

type Layer interface {
	SetTLSConfig(config *tls.Config)
	SetSASL(mechanism sasl.Mechanism)
}

func fillLayer(layer Layer, sasl *SASLConfig, loader *CertLoader) error {
	if sasl != nil {
		mechanism, err := sasl.Mechanism()
		if err != nil {
			return err
		}

		layer.SetSASL(mechanism)
	}

	if loader != nil {
		config, err := loader.TLSConfig()
		if err != nil {
			return err
		}

		layer.SetTLSConfig(config)
	}

	return nil
}
