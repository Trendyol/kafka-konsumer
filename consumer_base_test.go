package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/segmentio/kafka-go"
)

func Test_base_startConsume(t *testing.T) {
	t.Run("Return_When_Quit_Signal_Is_Came", func(t *testing.T) {
		mc := mockReader{wantErr: true}
		b := base{
			wg: sync.WaitGroup{}, r: &mc,
			incomingMessageStream: make(chan *Message),
			quit:                  make(chan struct{}),
			logger:                NewZapLogger(LogLevelDebug),
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
		b := base{wg: sync.WaitGroup{}, r: &mc, incomingMessageStream: make(chan *Message)}
		b.wg.Add(1)

		// When
		go b.startConsume()

		actual := <-b.incomingMessageStream

		// Then
		//nolint:lll
		expected := kafka.Message{Topic: "topic", Partition: 0, Offset: 1, HighWaterMark: 1, Key: []byte("foo"), Value: []byte("bar"), Headers: []kafka.Header{{Key: "header", Value: []byte("value")}}}

		if diff := cmp.Diff(actual.Headers[0], expected.Headers[0]); diff != "" {
			t.Error(diff)
		}
	})
}

type mockReader struct {
	wantErr bool
}

func (m *mockReader) FetchMessage(_ context.Context, msg *kafka.Message) error {
	if m.wantErr {
		return errors.New("err")
	}
	//nolint:lll
	*msg = kafka.Message{Topic: "topic", Partition: 0, Offset: 1, HighWaterMark: 1, Key: []byte("foo"), Value: []byte("bar"), Headers: []kafka.Header{{Key: "header", Value: []byte("value")}}}
	return nil
}

func (m *mockReader) Close() error {
	if m.wantErr {
		return errors.New("err")
	}
	return nil
}

func (m *mockReader) CommitMessages(_ []kafka.Message) error {
	if m.wantErr {
		return errors.New("err")
	}
	return nil
}
