package kafka

import "github.com/segmentio/kafka-go"

type Balancer kafka.Balancer

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
