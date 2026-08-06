package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func Test_consumer_startBatch(t *testing.T) {
	// Given
	var numberOfBatch atomic.Int64

	mc := mockReader{}
	c := consumer{
		base: &base{
			incomingMessageStream:  make(chan *IncomingMessage, 1),
			singleConsumingStream:  make(chan *Message, 1),
			messageProcessedStream: make(chan struct{}, 1),
			metric:                 &ConsumerMetric{},
			wg:                     sync.WaitGroup{},
			messageGroupDuration:   500 * time.Millisecond,
			r:                      &mc,
			concurrency:            1,
			logger:                 NewZapLogger(LogLevelDebug),
		},
		consumeFn: func(*Message) error {
			numberOfBatch.Add(1)
			return nil
		},
	}

	go func() {
		// Simulate concurrency of value 3
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Simulate messageGroupDuration
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Return from startBatch
		close(c.base.incomingMessageStream)
	}()

	c.base.wg.Add(1 + c.base.concurrency)

	// When
	c.setupConcurrentWorkers()
	c.startBatch()

	// Then
	if numberOfBatch.Load() != 3 {
		t.Fatalf("Number of batch group must equal to 3")
	}

	if c.metric.totalProcessedMessagesCounter != 3 {
		t.Fatalf("Total Processed Message Counter must equal to 3")
	}
}

func Test_consumer_process(t *testing.T) {
	t.Run("When_Processing_Is_Successful", func(t *testing.T) {
		// Given
		c := consumer{
			base: &base{metric: &ConsumerMetric{}},
			consumeFn: func(*Message) error {
				return nil
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.totalProcessedMessagesCounter != 1 {
			t.Fatalf("Total Processed Message Counter must equal to 3")
		}
		if c.metric.totalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Re-processing_Is_Successful", func(t *testing.T) {
		// Given
		gotOnlyOneTimeException := true
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), transactionalRetry: true},
			consumeFn: func(*Message) error {
				if gotOnlyOneTimeException {
					gotOnlyOneTimeException = false
					return errors.New("simulate only one time exception")
				}
				return nil
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.totalProcessedMessagesCounter != 1 {
			t.Fatalf("Total Processed Message Counter must equal to 1")
		}
		if c.metric.totalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Disabled", func(t *testing.T) {
		// Given
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug)},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.totalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if c.metric.totalUnprocessedMessagesCounter != 1 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 1")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Enabled", func(t *testing.T) {
		// Given
		mc := mockCronsumer{}
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.totalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if c.metric.totalUnprocessedMessagesCounter != 1 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 1")
		}
	})

	t.Run("When_Re-processing_Is_Failed_And_Retry_Failed", func(t *testing.T) {
		// Given
		mc := mockCronsumer{wantErr: true}
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()

		// When && Then
		c.process(&Message{})
	})

	t.Run("When_Re-processing_Is_Failed_And_Retry_Failed_5_times", func(t *testing.T) {
		// Given
		mc := mockCronsumer{wantErr: true, retryBehaviorOpen: true, maxRetry: 5}

		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
			if mc.times != mc.maxRetry {
				t.Errorf("Expected produce to be called %d times, but got %d", mc.maxRetry, mc.times)
			}
		}()

		// When && Then
		c.process(&Message{})
	})

	t.Run("When_SendDirectToDeadLetter_True_And_RetryEnabled_Should_Not_Call_Retry", func(t *testing.T) {
		// Given
		mdlp := &mockDeadLetterProducer{}
		mc := mockCronsumer{wantErr: true, retryBehaviorOpen: true, maxRetry: 5}
		c := consumer{
			base: &base{
				metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: mdlp,
				retryEnabled: true, cronsumer: &mc,
			},
			consumeFn: func(*Message) error { return errors.New("err occurred") },
		}
		msg := &Message{Key: []byte("1"), Value: []byte("foo"), SendDirectToDeadLetter: true}

		// When
		c.process(msg)

		// Then
		if mdlp.produceCalled != 1 {
			t.Fatalf("dead letter producer must be called once, got %d", mdlp.produceCalled)
		}
		if mc.times != 0 {
			t.Fatalf("retry cronsumer must not be called, got %d calls", mc.times)
		}
	})

	t.Run("When_SendDirectToDeadLetter_True_Should_Add_Error_Header_And_Metrics", func(t *testing.T) {
		// Given
		mdlp := &mockDeadLetterProducer{}
		c := consumer{
			base:      &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: mdlp},
			consumeFn: func(*Message) error { return errors.New("err occurred") },
		}
		msg := &Message{Key: []byte("1"), Value: []byte("foo"), SendDirectToDeadLetter: true}

		// When
		c.process(msg)

		// Then
		if mdlp.produceCalled != 1 {
			t.Fatalf("dead letter producer must be called once, got %d", mdlp.produceCalled)
		}
		if len(mdlp.received) != 1 {
			t.Fatalf("dead letter received length must be 1, got %d", len(mdlp.received))
		}
		produced := mdlp.received[0]
		if produced.Topic != "" {
			t.Fatalf("produced message Topic must be empty, got %q", produced.Topic)
		}
		assertErrHeader(t, produced, "err occurred")
		if c.metric.totalUnprocessedMessagesCounter != 1 {
			t.Fatalf("totalUnprocessedMessagesCounter must be 1, got %d", c.metric.totalUnprocessedMessagesCounter)
		}
		if c.metric.totalProcessedMessagesCounter != 0 {
			t.Fatalf("totalProcessedMessagesCounter must be 0, got %d", c.metric.totalProcessedMessagesCounter)
		}
	})

	t.Run("When_SendDirectToDeadLetter_True_Should_Use_ErrDescription_In_Header", func(t *testing.T) {
		// Given
		mdlp := &mockDeadLetterProducer{}
		c := consumer{
			base:      &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: mdlp},
			consumeFn: func(*Message) error { return errors.New("ignored by ErrDescription") },
		}
		msg := &Message{Key: []byte("2"), Value: []byte("bar"), SendDirectToDeadLetter: true, ErrDescription: "custom direct error"}

		// When
		c.process(msg)

		// Then
		if mdlp.produceCalled != 1 {
			t.Fatalf("dead letter producer must be called once, got %d", mdlp.produceCalled)
		}
		if len(mdlp.received) != 1 {
			t.Fatalf("dead letter received length must be 1, got %d", len(mdlp.received))
		}
		produced := mdlp.received[0]
		assertErrHeader(t, produced, "custom direct error")
	})

	t.Run("When_DeadLetter_Producer_Fails_Should_Panic_After_Backoff", func(t *testing.T) {
		// Given
		fdlp := &failingDeadLetterProducer{}
		c := consumer{
			base:      &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: fdlp},
			consumeFn: func(*Message) error { return errors.New("err occurred") },
		}
		msg := &Message{Key: []byte("1"), Value: []byte("foo"), SendDirectToDeadLetter: true}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
			if fdlp.called != 5 {
				t.Fatalf("dead letter producer must be called 5 times with backoff, got %d", fdlp.called)
			}
		}()

		// When && Then
		c.process(msg)
	})
}

func Test_consumer_Pause(t *testing.T) {
	// Given
	ctx, cancelFn := context.WithCancel(context.Background())
	c := consumer{
		base: &base{
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			context: ctx, cancelFn: cancelFn,
			consumerState: stateRunning,
		},
	}
	go func() {
		<-c.base.pause
	}()

	// When
	c.Pause()

	// Then
	if c.base.consumerState != statePaused {
		t.Fatal("consumer state must be in paused")
	}
}

func Test_consumer_Resume(t *testing.T) {
	// Given
	mc := mockReader{}
	ctx, cancelFn := context.WithCancel(context.Background())
	c := consumer{
		base: &base{
			r:       &mc,
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			quit:    make(chan struct{}),
			wg:      sync.WaitGroup{},
			context: ctx, cancelFn: cancelFn,
		},
	}

	// When
	c.Resume()

	// Then
	if c.base.consumerState != stateRunning {
		t.Fatal("consumer state must be in running")
	}
}

type mockDeadLetterProducer struct {
	received      []Message
	batches       [][]Message
	produceCalled int
}

func (m *mockDeadLetterProducer) Produce(_ context.Context, message Message) error {
	m.produceCalled++
	m.received = append(m.received, message)
	return nil
}

func (m *mockDeadLetterProducer) ProduceBatch(_ context.Context, messages []Message) error {
	m.produceCalled++
	m.received = append(m.received, messages...)
	m.batches = append(m.batches, append([]Message(nil), messages...))
	return nil
}

func (m *mockDeadLetterProducer) Close() error { return nil }

type failingDeadLetterProducer struct{ called int }

func (m *failingDeadLetterProducer) Produce(_ context.Context, _ Message) error {
	return errors.New("dlq produce fail")
}

func (m *failingDeadLetterProducer) ProduceBatch(_ context.Context, _ []Message) error {
	m.called++
	return errors.New("dlq produce batch fail")
}
func (m *failingDeadLetterProducer) Close() error { return nil }

func getHeaderValue(message Message, key string) (string, bool) {
	for _, h := range message.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func assertErrHeader(t *testing.T, message Message, expected string) {
	t.Helper()
	if v, ok := getHeaderValue(message, errMessageKey); !ok {
		t.Fatalf("%s header must be present on direct dead-lettered message", errMessageKey)
	} else if v != expected {
		t.Fatalf("%s must be %q, got %q", errMessageKey, expected, v)
	}
}
