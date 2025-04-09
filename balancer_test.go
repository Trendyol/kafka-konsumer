package kafka

import (
	"github.com/segmentio/kafka-go"
	"reflect"
	"testing"
)

func TestGetBalancerCRC32(t *testing.T) {
	balancer := GetBalancerCRC32()
	if balancer == nil {
		t.Error("Expected non-nil balancer, got nil")
	}
	if reflect.TypeOf(balancer).String() != "*kafka.CRC32Balancer" {
		t.Errorf("Expected *kafka.CRC32Balancer, got %s", reflect.TypeOf(balancer).String())
	}
}

func TestGetBalancerHash(t *testing.T) {
	balancer := GetBalancerHash()
	if balancer == nil {
		t.Error("Expected non-nil balancer, got nil")
	}
	if reflect.TypeOf(balancer).String() != "*kafka.Hash" {
		t.Errorf("Expected *kafka.Hash, got %s", reflect.TypeOf(balancer).String())
	}
}

func TestGetBalancerLeastBytes(t *testing.T) {
	balancer := GetBalancerLeastBytes()
	if balancer == nil {
		t.Error("Expected non-nil balancer, got nil")
	}
	if reflect.TypeOf(balancer).String() != "*kafka.LeastBytes" {
		t.Errorf("Expected *kafka.LeastBytes, got %s", reflect.TypeOf(balancer).String())
	}
}

func TestGetBalancerMurmur2Balancer(t *testing.T) {
	balancer := GetBalancerMurmur2Balancer()
	if balancer == nil {
		t.Error("Expected non-nil balancer, got nil")
	}
	if reflect.TypeOf(balancer).String() != "*kafka.Murmur2Balancer" {
		t.Errorf("Expected *kafka.Murmur2Balancer, got %s", reflect.TypeOf(balancer).String())
	}
}

func TestGetBalancerReferenceHash(t *testing.T) {
	balancer := GetBalancerReferenceHash()
	if balancer == nil {
		t.Error("Expected non-nil balancer, got nil")
	}
	if reflect.TypeOf(balancer).String() != "*kafka.ReferenceHash" {
		t.Errorf("Expected *kafka.ReferenceHash, got %s", reflect.TypeOf(balancer).String())
	}
}

func TestGetBalancerRoundRobinh(t *testing.T) {
	balancer := GetBalancerRoundRobin()
	if balancer == nil {
		t.Error("Expected non-nil balancer, got nil")
	}
	if reflect.TypeOf(balancer).String() != "*kafka.RoundRobin" {
		t.Errorf("Expected *kafka.RoundRobin, got %s", reflect.TypeOf(balancer).String())
	}
}

func TestGetBalancerString(t *testing.T) {
	tests := []struct {
		name     string
		balancer Balancer
		want     string
	}{
		{
			name:     "Should_Return_CRC32Balancer",
			balancer: GetBalancerCRC32(),
			want:     "CRC32Balancer",
		},
		{
			name:     "Should_Return_Hash",
			balancer: GetBalancerHash(),
			want:     "Hash",
		},
		{
			name:     "Should_Return_LeastBytes",
			balancer: GetBalancerLeastBytes(),
			want:     "LeastBytes",
		},
		{
			name:     "Should_Return_Murmur2Balancer",
			balancer: GetBalancerMurmur2Balancer(),
			want:     "Murmur2Balancer",
		},
		{
			name:     "Should_Return_ReferenceHash",
			balancer: GetBalancerReferenceHash(),
			want:     "ReferenceHash",
		},
		{
			name:     "Should_Return_RoundRobin",
			balancer: GetBalancerRoundRobin(),
			want:     "RoundRobin",
		},
		{
			name:     "Should_Return_Unknown",
			balancer: nil,
			want:     "Unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetBalancerString(tt.balancer); got != tt.want {
				t.Errorf("GetBalancerString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultBalancer_Balance(t *testing.T) {
	partitions := []int{0, 1, 2, 3}

	t.Run("Should_Use_RoundRobin_When_Key_Is_Nil", func(t *testing.T) {
		// Given
		msg := kafka.Message{Key: nil}
		balancer := &defaultBalancer{}
		expected := GetBalancerRoundRobin().Balance(msg, partitions...)

		// When
		result := balancer.Balance(msg, partitions...)

		// Then
		if result != expected {
			t.Errorf("Expected RoundRobin partition %d, got %d", expected, result)
		}
	})

	t.Run("Should_Use_Murmur2_When_Key_Is_Not_Nil", func(t *testing.T) {
		// Given
		msg := kafka.Message{Key: []byte("key")}
		balancer := &defaultBalancer{}
		expected := GetBalancerMurmur2Balancer().Balance(msg, partitions...)

		// When
		result := balancer.Balance(msg, partitions...)

		// Then
		if result != expected {
			t.Errorf("Expected Murmur2Balancer partition %d, got %d", expected, result)
		}
	})
}

type optimizedBalancer struct{}

func (s *optimizedBalancer) Balance(msg kafka.Message, partitions ...int) int {
	var balancer kafka.Balancer
	if msg.Key == nil {
		balancer = balancerRoundRobin
	} else {
		balancer = balancerMurmur
	}
	return balancer.Balance(msg, partitions...)
}

func BenchmarkDefaultBalancer_WithAlloc(b *testing.B) {
	partitions := []int{0, 1, 2, 3}
	msgWithKey := kafka.Message{Key: []byte("key")}
	msgWithoutKey := kafka.Message{Key: nil}
	balancer := &defaultBalancer{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			balancer.Balance(msgWithKey, partitions...)
		} else {
			balancer.Balance(msgWithoutKey, partitions...)
		}
	}
}

func BenchmarkDefaultBalancer_Optimized(b *testing.B) {
	partitions := []int{0, 1, 2, 3}
	msgWithKey := kafka.Message{Key: []byte("key")}
	msgWithoutKey := kafka.Message{Key: nil}
	balancer := &optimizedBalancer{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			balancer.Balance(msgWithKey, partitions...)
		} else {
			balancer.Balance(msgWithoutKey, partitions...)
		}
	}
}

func BenchmarkDefaultBalancer_Direct(b *testing.B) {
	partitions := []int{0, 1, 2, 3}
	msgWithKey := kafka.Message{Key: []byte("key")}
	msgWithoutKey := kafka.Message{Key: nil}
	balancer := &optimizedBalancer{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			balancer.Balance(msgWithKey, partitions...)
		} else {
			balancer.Balance(msgWithoutKey, partitions...)
		}
	}
}
