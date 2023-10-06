package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func Test_otelProducer_WriteMessages(t *testing.T) {
	t.Run("Return_Success_When_Single_Message_Producing", func(t *testing.T) {
		// Given
		o := otelProducer{w: mockOtelKafkaKonsumerWriter{wantErr: false}}

		// When
		err := o.WriteMessages(context.TODO(), kafka.Message{})
		// Then
		if err != nil {
			t.Fatalf("Error when single producing %v", err)
		}
	})
	t.Run("Return_Error_When_Single_Message_Producing", func(t *testing.T) {
		// Given
		o := otelProducer{w: mockOtelKafkaKonsumerWriter{wantErr: true}}

		// When
		err := o.WriteMessages(context.TODO(), kafka.Message{})

		// Then
		if err == nil {
			t.Fatalf("Success when single producing %v", err)
		}
	})
	t.Run("Return_Success_When_Batch_Producing", func(t *testing.T) {
		// Given
		o := otelProducer{w: mockOtelKafkaKonsumerWriter{wantErr: false}}

		// When
		err := o.WriteMessages(context.TODO(), kafka.Message{}, kafka.Message{})
		// Then
		if err != nil {
			t.Fatalf("Error when batch producing %v", err)
		}
	})
	t.Run("Return_Error_When_Batch_Producing", func(t *testing.T) {
		// Given
		o := otelProducer{w: mockOtelKafkaKonsumerWriter{wantErr: true}}

		// When
		err := o.WriteMessages(context.TODO(), kafka.Message{}, kafka.Message{})

		// Then
		if err == nil {
			t.Fatalf("Success when single producing %v", err)
		}
	})
}

type mockOtelKafkaKonsumerWriter struct {
	wantErr bool
}

var _ OtelKafkaKonsumerWriter = (*mockOtelKafkaKonsumerWriter)(nil)

func (m mockOtelKafkaKonsumerWriter) WriteMessage(ctx context.Context, msg kafka.Message) error {
	if m.wantErr {
		return errors.New("err occurred")
	}
	return nil
}

func (m mockOtelKafkaKonsumerWriter) WriteMessages(ctx context.Context, msgs []kafka.Message) error {
	if m.wantErr {
		return errors.New("err occurred")
	}
	return nil
}

func (m mockOtelKafkaKonsumerWriter) Close() error {

	panic("implement me")
}
