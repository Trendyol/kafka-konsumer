package kafka

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

func Test_producer_Produce_Successfully(t *testing.T) {
	// Given
	mw := &mockWriter{}
	p := producer{w: mw}

	// When
	err := p.Produce(context.Background(), Message{})
	// Then
	if err != nil {
		t.Fatalf("Producing err %s", err.Error())
	}
}

func Test_producer_ProduceBatch_Successfully(t *testing.T) {
	// Given
	mw := &mockWriter{}
	p := producer{w: mw}

	// When
	err := p.ProduceBatch(context.Background(), []Message{{}, {}, {}})
	// Then
	if err != nil {
		t.Fatalf("Batch Producing err %s", err.Error())
	}
}

func Test_producer_Close_Successfully(t *testing.T) {
	// Given
	mw := &mockWriter{}
	p := producer{w: mw}

	// When
	err := p.Close()
	// Then
	if err != nil {
		t.Fatalf("Closing err %s", err.Error())
	}
}

type mockWriter struct{}

func (m *mockWriter) WriteMessages(ctx context.Context, message ...kafka.Message) error {
	return nil
}

func (m *mockWriter) Close() error {
	return nil
}
