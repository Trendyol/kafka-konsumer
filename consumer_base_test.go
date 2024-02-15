package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/segmentio/kafka-go"
)

func Test_base_startConsume(t *testing.T) {
	t.Run("Return_When_Quit_Signal_Is_Came", func(t *testing.T) {
		mc := mockReader{wantErr: true}
		b := base{
			wg:                    sync.WaitGroup{},
			r:                     &mc,
			incomingMessageStream: make(chan *IncomingMessage),
			quit:                  make(chan struct{}),
			pause:                 make(chan struct{}),
			logger:                NewZapLogger(LogLevelError),
			consumerState:         stateRunning,
		}
		b.context, b.cancelFn = context.WithCancel(context.Background())

		b.wg.Add(1)

		// When
		go b.startConsume()

		// Ensure some time passes
		time.Sleep(3 * time.Second)
		b.quit <- struct{}{}

		// Then
		// Ensure quit called, it works because defer wg.Done statement
		b.wg.Wait()
	})
	t.Run("Read_Incoming_Messages_Successfully", func(t *testing.T) {
		// Given
		mc := mockReader{}
		b := base{wg: sync.WaitGroup{}, r: &mc, incomingMessageStream: make(chan *IncomingMessage)}
		b.wg.Add(1)

		// When
		go b.startConsume()

		actual := <-b.incomingMessageStream

		// Then
		//nolint:lll
		expected := kafka.Message{Topic: "topic", Partition: 0, Offset: 1, HighWaterMark: 1, Key: []byte("foo"), Value: []byte("bar"), Headers: []kafka.Header{{Key: "header", Value: []byte("value")}}}

		if diff := cmp.Diff(actual.message.Headers[0], expected.Headers[0]); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("Skip_Incoming_Messages_When_SkipMessageByHeaderFn_Is_Applied", func(t *testing.T) {
		// Given
		mc := mockReader{}
		skipMessageCh := make(chan struct{})
		b := base{
			wg:                    sync.WaitGroup{},
			r:                     &mc,
			logger:                NewZapLogger(LogLevelDebug),
			incomingMessageStream: make(chan *IncomingMessage),
			skipMessageByHeaderFn: func(header []kafka.Header) bool {
				defer func() {
					skipMessageCh <- struct{}{}
				}()

				for _, h := range header {
					if h.Key == "header" {
						return true
					}
				}
				return false
			},
		}

		b.wg.Add(1)

		// When
		go b.startConsume()

		// Then
		<-skipMessageCh

		// assert incomingMessageStream does not receive any value because message is skipped
		select {
		case <-b.incomingMessageStream:
			t.Fatal("incoming message stream must equal to 0")
		case <-time.After(1 * time.Second):
		}
	})
}

func Test_base_Pause(t *testing.T) {
	// Given
	ctx, cancelFn := context.WithCancel(context.Background())
	b := base{
		logger:  NewZapLogger(LogLevelDebug),
		pause:   make(chan struct{}),
		context: ctx, cancelFn: cancelFn,
		consumerState: stateRunning,
	}
	go func() {
		<-b.pause
	}()

	// When
	b.Pause()

	// Then
	if b.consumerState != statePaused {
		t.Fatal("consumer state must be in paused")
	}
}

func Test_base_Resume(t *testing.T) {
	// Given
	mc := mockReader{}
	ctx, cancelFn := context.WithCancel(context.Background())
	b := base{
		r:       &mc,
		logger:  NewZapLogger(LogLevelDebug),
		pause:   make(chan struct{}),
		quit:    make(chan struct{}),
		wg:      sync.WaitGroup{},
		context: ctx, cancelFn: cancelFn,
	}

	// When
	b.Resume()

	// Then
	if b.consumerState != stateRunning {
		t.Fatal("consumer state must be in running")
	}
	if ctx == b.context {
		t.Fatal("contexts must be differ!")
	}
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
