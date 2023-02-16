package kafka

import (
	"crypto/tls"
	"crypto/x509"
)

type CertLoader struct {
	RootCA  func() ([]byte, error)
	InterCA func() ([]byte, error)
}

func (c CertLoader) TLSConfig() (*tls.Config, error) {
	caCertPool := x509.NewCertPool()
	appendCAWithLoader := func(loader func() ([]byte, error)) error {
		if loader == nil {
			return nil
		}

		bytes, err := loader()
		if err != nil {
			return err
		}

		caCertPool.AppendCertsFromPEM(bytes)
		return nil
	}

	if err := appendCAWithLoader(c.RootCA); err != nil {
		return nil, err
	}

	if err := appendCAWithLoader(c.InterCA); err != nil {
		return nil, err
	}

	return &tls.Config{RootCAs: caCertPool}, nil
}
