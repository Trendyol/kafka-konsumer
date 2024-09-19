package kafka

import (
	"context"
	stubData "github.com/Trendyol/kafka-konsumer/v2/test/stub-data"
	"github.com/gofiber/fiber/v2/utils"
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

func Test_producer_Produce_interceptor_Successfully(t *testing.T) {
	// Given
	mw := &mockWriter{}
	msg := Message{Headers: make([]Header, 0)}
	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   "x-correlation-id",
		Value: []byte(utils.UUIDv4()),
	})
	interceptor := stubData.NewMockProducerInterceptor()

	p := producer{w: mw, interceptor: &interceptor}

	// When
	err := p.Produce(context.Background(), msg)

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

func Test_producer_ProduceBatch_interceptor_Successfully(t *testing.T) {
	// Given
	mw := &mockWriter{}
	interceptor := stubData.NewMockProducerInterceptor()
	p := producer{w: mw, interceptor: &interceptor}

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

func (m *mockWriter) WriteMessages(_ context.Context, msg ...kafka.Message) error {
	return nil
}

func (m *mockWriter) Close() error {
	return nil
}
