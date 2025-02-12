package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	RootCAPath         string
	IntermediateCAPath string
}

func (c *TLSConfig) TLSConfig(logger LoggerInterface) (*tls.Config, error) {
	rootCA, err := os.ReadFile(c.RootCAPath)
	if err != nil {
		return nil, fmt.Errorf("Error while reading Root CA file: " + c.RootCAPath + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(rootCA); !ok {
		return nil, fmt.Errorf("failed to append Root CA certificates from file: %s", c.RootCAPath)
	}

	interCA, err := os.ReadFile(c.IntermediateCAPath)
	if err != nil {
		logger.Warnf("Unable to read Intermediate CA file: %s, error: %v", c.IntermediateCAPath, err)
		logger.Info("Intermediate CA will be skipped.")
	} else if ok := caCertPool.AppendCertsFromPEM(interCA); !ok {
		logger.Warnf("Failed to append Intermediate CA certificates from file: %s", c.IntermediateCAPath)
	}

	return &tls.Config{RootCAs: caCertPool}, nil //nolint:gosec
}

func (c *TLSConfig) IsEmpty() bool {
	return c == nil || c.RootCAPath == "" && c.IntermediateCAPath == ""
}

func (c *TLSConfig) JSON() string {
	if c == nil {
		return "{}"
	}
	return fmt.Sprintf(`{"RootCAPath": %q, "IntermediateCAPath": %q}`, c.RootCAPath, c.IntermediateCAPath)
}
