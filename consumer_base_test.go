package kafka

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
)

func Test_base_startConsume(t *testing.T) {
	t.Run("Return_When_Quit_Signal_Is_Came", func(t *testing.T) {
		mc := mockReader{wantErr: true}
		b := base{
			wg: sync.WaitGroup{}, r: &mc,
			messageCh: make(chan *Message),
			quit:      make(chan struct{}),
			logger:    NewZapLogger(LogLevelDebug),
		}
		b.context, b.cancelFn = context.WithCancel(context.Background())

		b.wg.Add(1)

		// When
		go b.startConsume()
		b.quit <- struct{}{}

		// Then
		// Ensure quit called, it works because defer wg.Done statement
		b.wg.Wait()
	})
	t.Run("Read_Incoming_Messages_Successfully", func(t *testing.T) {
		// Given
		mc := mockReader{}
		b := base{wg: sync.WaitGroup{}, r: &mc, messageCh: make(chan *Message)}
		b.wg.Add(1)

		// When
		go b.startConsume()

		actual := <-b.messageCh

		// Then
		//nolint:lll
		expected := kafka.Message{Topic: "topic", Partition: 0, Offset: 1, HighWaterMark: 1, Key: []byte("foo"), Value: []byte("bar"), Headers: []kafka.Header{{Key: "header", Value: []byte("value")}}}

		actualHeader := actual.Headers[0]
		expectedHeader := expected.Headers[0]

		if actual.Topic != expected.Topic {
			t.Errorf("Expected: %s, Actual: %s", expected.Topic, actual.Topic)
		}
		if actual.Partition != expected.Partition {
			t.Errorf("Expected: %d, Actual: %d", expected.Partition, actual.Partition)
		}
		if actual.Offset != expected.Offset {
			t.Errorf("Expected: %d, Actual: %d", expected.Offset, actual.Offset)
		}
		if actual.HighWaterMark != expected.HighWaterMark {
			t.Errorf("Expected: %d, Actual: %d", expected.HighWaterMark, actual.HighWaterMark)
		}
		if !bytes.Equal(actual.Key, expected.Key) {
			t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
		}
		if !bytes.Equal(actual.Value, expected.Value) {
			t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
		}
		if actualHeader.Key != expectedHeader.Key {
			t.Errorf("Expected: %s, Actual: %s", actualHeader.Key, expectedHeader.Key)
		}
		if !bytes.Equal(actualHeader.Value, expectedHeader.Value) {
			t.Errorf("Expected: %s, Actual: %s", expectedHeader.Value, expectedHeader.Value)
		}
		if actual.Time != expected.Time {
			t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
		}
	})
}

type mockReader struct {
	wantErr bool
}

func (m *mockReader) FetchMessage(ctx context.Context) (*kafka.Message, error) {
	if m.wantErr {
		return nil, errors.New("err")
	}
	//nolint:lll
	return &kafka.Message{Topic: "topic", Partition: 0, Offset: 1, HighWaterMark: 1, Key: []byte("foo"), Value: []byte("bar"), Headers: []kafka.Header{{Key: "header", Value: []byte("value")}}}, nil
}

func (m *mockReader) Close() error {
	panic("implement me")
}

func (m *mockReader) CommitMessages(messages []kafka.Message) error {
	panic("implement me")
}
