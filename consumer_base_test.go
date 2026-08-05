package kafka

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/segmentio/kafka-go"
)

func Test_base_startConsume(t *testing.T) {
	t.Run("Return_When_Quit_Signal_Is_Came", func(_ *testing.T) {
		mc := mockReader{wantErr: true}
		b := base{
			wg:                    sync.WaitGroup{},
			r:                     &mc,
			incomingMessageStream: make(chan *IncomingMessage),
			quit:                  make(chan struct{}),
			pause:                 make(chan struct{}),
			logger:                NewZapLogger(LogLevelError),
			consumerState:         stateRunning,
			metric:                &ConsumerMetric{},
			consumerCfg:           &ConsumerConfig{},
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
	t.Run("Call_One_Goroutine", func(t *testing.T) {
		// Given
		ctx, cancelFn := context.WithCancel(context.Background())
		b := base{
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			context: ctx, cancelFn: cancelFn,
			consumerState: stateRunning,
			mu:            sync.Mutex{},
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
	})
	t.Run("Call_Multiple_Goroutine", func(t *testing.T) {
		// Given
		ctx, cancelFn := context.WithCancel(context.Background())
		b := base{
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			context: ctx, cancelFn: cancelFn,
			consumerState: stateRunning,
			mu:            sync.Mutex{},
		}
		go func() {
			<-b.pause
		}()

		// When
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			b.Pause()
			wg.Done()
		}()
		go func() {
			b.Pause()
			wg.Done()
		}()
		wg.Wait()

		// Then
		if b.consumerState != statePaused {
			t.Fatal("consumer state must be in paused")
		}
	})
}

func Test_base_Resume(t *testing.T) {
	t.Run("Call_One_Goroutine", func(t *testing.T) {
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
			mu: sync.Mutex{},
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
	})
	t.Run("Call_Multiple_Goroutine", func(t *testing.T) {
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
			mu: sync.Mutex{},
		}

		// When
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			b.Resume()
			wg.Done()
		}()
		go func() {
			b.Resume()
			wg.Done()
		}()
		wg.Wait()

		// Then
		if b.consumerState != stateRunning {
			t.Fatal("consumer state must be in running")
		}
		if ctx == b.context {
			t.Fatal("contexts must be differ!")
		}
	})
}

func Test_initializeDeadLetterProducer(t *testing.T) {
	t.Run("Should_Set_Producer_Compression", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{
			ClientID:                     "client-id",
			DeadLetterTopic:              "dead-letter-topic",
			DeadLetterProducerBatchBytes: 1024,
			Reader: ReaderConfig{
				Brokers: []string{"broker-1.test.com"},
			},
			RetryConfiguration: RetryConfiguration{
				ProducerCompression: kafka.Gzip,
			},
		}

		// When
		deadLetterProducer, err := initializeDeadLetterProducer(&cfg)

		// Then
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		p, ok := deadLetterProducer.(*producer)
		if !ok {
			t.Fatalf("expected *producer, got %T", deadLetterProducer)
		}

		writer, ok := p.w.(*kafka.Writer)
		if !ok {
			t.Fatalf("expected *kafka.Writer, got %T", p.w)
		}

		if writer.Compression != kafka.Gzip {
			t.Errorf("expected Compression gzip, got %s", writer.Compression)
		}
		if writer.BatchBytes != math.MaxInt {
			t.Errorf("expected BatchBytes %d, got %d", math.MaxInt, writer.BatchBytes)
		}
	})
}

func Test_base_sendToDeadLetterWithBackoff(t *testing.T) {
	t.Run("Should_Call_ProduceBatch_Once_When_BatchBytes_Is_Disabled", func(t *testing.T) {
		// Given
		producer := &mockDeadLetterProducer{}
		b := base{
			deadLetterProducer: producer,
			logger:             NewZapLogger(LogLevelError),
		}
		messages := []Message{
			{Value: []byte("aaaa")},
			{Value: []byte("bbbb")},
			{Value: []byte("cccc")},
		}

		// When
		err := b.sendToDeadLetterWithBackoff(messages...)

		// Then
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if producer.produceCalled != 1 {
			t.Fatalf("dead letter producer must be called once, got %d", producer.produceCalled)
		}
		if len(producer.batches) != 1 || len(producer.batches[0]) != 3 {
			t.Fatalf("expected one batch with 3 messages, got %#v", producer.batches)
		}
	})

	t.Run("Should_Chunk_Messages_By_BatchBytes", func(t *testing.T) {
		// Given
		producer := &mockDeadLetterProducer{}
		messages := []Message{
			{Key: []byte("1"), Value: []byte("aaaa")},
			{Key: []byte("2"), Value: []byte("bbbb")},
			{Key: []byte("3"), Value: []byte("cccc")},
		}
		b := base{
			deadLetterProducer:           producer,
			deadLetterProducerBatchBytes: int64(messages[0].TotalSize() + messages[1].TotalSize()),
			logger:                       NewZapLogger(LogLevelError),
		}

		// When
		err := b.sendToDeadLetterWithBackoff(messages...)

		// Then
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if producer.produceCalled != 2 {
			t.Fatalf("dead letter producer must be called twice, got %d", producer.produceCalled)
		}
		assertBatchKeys(t, producer.batches, [][]string{{"1", "2"}, {"3"}})
	})

	t.Run("Should_Send_Single_Message_Larger_Than_Limit_Alone", func(t *testing.T) {
		// Given
		producer := &mockDeadLetterProducer{}
		messages := []Message{
			{Key: []byte("oversized"), Value: []byte("aaaaaaaaaa")},
			{Key: []byte("small-1"), Value: []byte("b")},
			{Key: []byte("small-2"), Value: []byte("c")},
		}
		b := base{
			deadLetterProducer:           producer,
			deadLetterProducerBatchBytes: int64(messages[1].TotalSize() + messages[2].TotalSize()),
			logger:                       NewZapLogger(LogLevelError),
		}

		// When
		err := b.sendToDeadLetterWithBackoff(messages...)

		// Then
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		assertBatchKeys(t, producer.batches, [][]string{{"oversized"}, {"small-1", "small-2"}})
	})

	t.Run("Should_Return_Error_And_Stop_When_Chunk_Fails", func(t *testing.T) {
		// Given
		expectedErr := errors.New("dlq chunk failed")
		producer := &failingOnBatchDeadLetterProducer{failBatch: 2, err: expectedErr}
		messages := []Message{
			{Key: []byte("1"), Value: []byte("aaaa")},
			{Key: []byte("2"), Value: []byte("bbbb")},
			{Key: []byte("3"), Value: []byte("cccc")},
		}
		b := base{
			deadLetterProducer:           producer,
			deadLetterProducerBatchBytes: int64(messages[0].TotalSize()),
			logger:                       NewZapLogger(LogLevelError),
		}

		// When
		err := b.sendToDeadLetterWithBackoff(messages...)

		// Then
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected %v, got %v", expectedErr, err)
		}
		if !strings.Contains(err.Error(), "messages=1") || !strings.Contains(err.Error(), "approxBytes=") ||
			!strings.Contains(err.Error(), `firstMessageKey="2"`) {
			t.Fatalf("expected chunk details in error, got %v", err)
		}
		if len(producer.successfulBatches) != 1 {
			t.Fatalf("expected only first chunk to be produced successfully, got %d", len(producer.successfulBatches))
		}
		if producer.called != 6 {
			t.Fatalf("expected 6 ProduceBatch calls (1 success + 5 retries), got %d", producer.called)
		}
	})
}

func Test_chunkMessagesByBytes(t *testing.T) {
	messages := []Message{
		{Key: []byte("1"), Value: []byte("aaaa")},
		{Key: []byte("2"), Value: []byte("bbbb")},
		{Key: []byte("3"), Value: []byte("cccc")},
	}

	chunks := chunkMessagesByBytes(messages, int64(messages[0].TotalSize()+messages[1].TotalSize()))

	assertBatchKeys(t, chunks, [][]string{{"1", "2"}, {"3"}})
}

func Test_drainTimer(t *testing.T) {
	// Test case 1: Timer expires before calling drainTimer
	t1 := time.NewTimer(10 * time.Millisecond)
	time.Sleep(20 * time.Millisecond) // Ensure the timer has expired
	drainTimer(t1)
	select {
	case <-t1.C:
		t.Error("Timer channel should be drained but is not.")
	default:
		// Success, the channel is drained
	}

	// clear timer state for test case 2
	t1.Reset(50 * time.Millisecond)

	// Test case 2: Timer is still active when calling drainTimer
	drainTimer(t1)
	select {
	case <-t1.C:
		// Timer should still expire normally
	case <-time.After(100 * time.Millisecond):
		t.Error("Timer did not expire as expected.")
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

type failingOnBatchDeadLetterProducer struct {
	called            int
	failBatch         int
	err               error
	successfulBatches [][]Message
}

func (m *failingOnBatchDeadLetterProducer) Produce(_ context.Context, message Message) error {
	return m.ProduceBatch(context.Background(), []Message{message})
}

func (m *failingOnBatchDeadLetterProducer) ProduceBatch(_ context.Context, messages []Message) error {
	m.called++
	if len(m.successfulBatches)+1 == m.failBatch {
		return m.err
	}

	batch := append([]Message(nil), messages...)
	m.successfulBatches = append(m.successfulBatches, batch)
	return nil
}

func (m *failingOnBatchDeadLetterProducer) Close() error { return nil }

func assertBatchKeys(t *testing.T, batches [][]Message, expected [][]string) {
	t.Helper()

	if len(batches) != len(expected) {
		t.Fatalf("expected %d batches, got %d", len(expected), len(batches))
	}

	for i := range expected {
		if len(batches[i]) != len(expected[i]) {
			t.Fatalf("expected batch %d to have %d messages, got %d", i, len(expected[i]), len(batches[i]))
		}
		for j := range expected[i] {
			if string(batches[i][j].Key) != expected[i][j] {
				t.Fatalf("expected batch %d message %d key %q, got %q", i, j, expected[i][j], string(batches[i][j].Key))
			}
		}
	}
}
