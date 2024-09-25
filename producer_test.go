package kafka

import (
	"context"
	"testing"

	"github.com/gofiber/fiber/v2/utils"

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
	interceptor := newMockProducerInterceptor()

	p := producer{w: mw, interceptors: interceptor}

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
	interceptor := newMockProducerInterceptor()
	p := producer{w: mw, interceptors: interceptor}

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

type mockProducerInterceptor struct{}

func (i *mockProducerInterceptor) OnProduce(ctx ProducerInterceptorContext) {
	ctx.Message.Headers = append(ctx.Message.Headers, kafka.Header{
		Key:   "test",
		Value: []byte("test"),
	})
}

func newMockProducerInterceptor() []ProducerInterceptor {
	return []ProducerInterceptor{&mockProducerInterceptor{}}
}
