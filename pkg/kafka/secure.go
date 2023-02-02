package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Abdulsametileri/kafka-template/pkg/config"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
)

func newTLSConfig(kafkaCfg *config.Kafka) (*tls.Config, error) {
	rootCA, err := os.ReadFile(kafkaCfg.RootCAPath)
	if err != nil {
		return nil, fmt.Errorf("reading RootCAPath file error: %w", err)
	}

	interCA, err := os.ReadFile(kafkaCfg.IntermediateCAPath)
	if err != nil {
		return nil, fmt.Errorf("reading IntermediateCAPath file error: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)
	caCertPool.AppendCertsFromPEM(interCA)

	return &tls.Config{RootCAs: caCertPool}, nil
}

func newMechanism(kafkaCfg *config.Kafka) (sasl.Mechanism, error) {
	return scram.Mechanism(scram.SHA512, kafkaCfg.ScramUsername, kafkaCfg.ScramPassword)
}
