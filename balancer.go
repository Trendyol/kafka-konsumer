package kafka

import "github.com/segmentio/kafka-go"

type Balancer kafka.Balancer

type DefaultBalancer struct{}

func (s *DefaultBalancer) Balance(msg kafka.Message, partitions ...int) (partition int) {
	if msg.Key == nil {
		return GetBalancerRoundRobin().Balance(msg, partitions...)
	}
	return GetBalancerMurmur2Balancer().Balance(msg, partitions...)
}

func GetBalancerCRC32() Balancer {
	return &kafka.CRC32Balancer{}
}

func GetBalancerHash() Balancer {
	return &kafka.Hash{}
}

func GetBalancerLeastBytes() Balancer {
	return &kafka.LeastBytes{}
}

func GetBalancerMurmur2Balancer() Balancer {
	return &kafka.Murmur2Balancer{}
}

func GetBalancerReferenceHash() Balancer {
	return &kafka.ReferenceHash{}
}

func GetBalancerRoundRobin() Balancer {
	return &kafka.RoundRobin{}
}

func GetBalancerString(balancer Balancer) string {
	switch balancer.(type) {
	case *kafka.CRC32Balancer:
		return "CRC32Balancer"
	case *kafka.Hash:
		return "Hash"
	case *kafka.LeastBytes:
		return "LeastBytes"
	case *kafka.Murmur2Balancer:
		return "Murmur2Balancer"
	case *kafka.ReferenceHash:
		return "ReferenceHash"
	case *kafka.RoundRobin:
		return "RoundRobin"
	default:
		return "Unknown"
	}
}
