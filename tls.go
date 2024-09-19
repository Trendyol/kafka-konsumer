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

func (c *TLSConfig) TLSConfig() (*tls.Config, error) {
	rootCA, err := os.ReadFile(c.RootCAPath)
	if err != nil {
		return nil, fmt.Errorf("Error while reading Root CA file: " + c.RootCAPath + " error: " + err.Error())
	}

	interCA, err := os.ReadFile(c.IntermediateCAPath)
	if err != nil {
		return nil, fmt.Errorf("Error while reading Intermediate CA file: " + c.IntermediateCAPath + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)
	caCertPool.AppendCertsFromPEM(interCA)

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
