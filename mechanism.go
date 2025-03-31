package kafka

import (
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Mechanism string

const (
	MechanismScram       = "scram"
	MechanismScramSHA256 = "scram-sha-256"
	MechanismScramSHA512 = "scram-sha-512"
	MechanismPlain       = "plain"
)

type SASLConfig struct {
	Type     Mechanism
	Username string
	Password string
}

func (s *SASLConfig) Mechanism() (sasl.Mechanism, error) {
	if s.Type == MechanismScram || s.Type == MechanismScramSHA512 {
		return scram.Mechanism(scram.SHA512, s.Username, s.Password)
	}

	if s.Type == MechanismScramSHA256 {
		return scram.Mechanism(scram.SHA256, s.Username, s.Password)
	}

	return s.plain(), nil
}

func (s *SASLConfig) plain() sasl.Mechanism {
	return &plain.Mechanism{
		Username: s.Username,
		Password: s.Password,
	}
}

func (s *SASLConfig) IsEmpty() bool {
	return s == nil
}

func (s *SASLConfig) JSON() string {
	if s == nil {
		return "{}"
	}
	return fmt.Sprintf(`{"Mechanism": %q, "Username": %q, "Password": %q}`, s.Type, s.Username, s.Password)
}
