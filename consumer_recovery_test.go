package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestConsumer_Process_RecoversPanics(t *testing.T) {
	for _, value := range []any{"handler panic", kafka.TopicAuthorizationFailed, nil} {
		t.Run("handler", func(t *testing.T) {
			c := &consumer{
				base:      &base{metric: &ConsumerMetric{}},
				consumeFn: func(*Message) error { panic(value) },
			}
			err := c.process(&Message{Topic: "source", Partition: 2, Offset: 7})
			var failure *processError
			if !errors.As(err, &failure) || failure.Stage != "consume" || len(failure.Stack) == 0 {
				t.Fatalf("expected recovered handler panic, got %v", err)
			}
			if failure.SourceTopic != "source" || failure.Partition != 2 || failure.Offset != 7 {
				t.Fatalf("source metadata was lost: %+v", failure)
			}
		})
	}
}

func TestConsumer_Process_ReturnsDLQAuthorizationError(t *testing.T) {
	producer := &recoveryProducer{err: kafka.TopicAuthorizationFailed}
	c := &consumer{
		base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelError),
			deadLetterProducer: producer, deadLetterTopic: "dead-letter"},
		consumeFn: func(*Message) error { return errors.New("business failure") },
	}
	message := &Message{Topic: "source", Partition: 2, Offset: 7, SendDirectToDeadLetter: true,
		Headers: Headers{{Key: errMessageKey, Value: []byte("original")}}}

	err := c.process(message)

	var failure *processError
	if !errors.As(err, &failure) || !errors.Is(err, kafka.TopicAuthorizationFailed) {
		t.Fatalf("expected original Kafka authorization error, got %v", err)
	}
	if failure.Stage != "dlq" || failure.TargetTopic != "dead-letter" || failure.SourceTopic != "source" {
		t.Fatalf("wrong delivery metadata: %+v", failure)
	}
	if producer.calls.Load() != 5 || message.Topic != "source" || string(message.Headers[0].Value) != "original" {
		t.Fatal("backoff or immutable source message contract was not preserved")
	}
}

func TestConsumer_Process_RecoversDLQProducerPanic(t *testing.T) {
	c := &consumer{
		base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelError),
			deadLetterProducer: &recoveryProducer{panicValue: kafka.TopicAuthorizationFailed}, deadLetterTopic: "dead-letter"},
		consumeFn: func(*Message) error { return errors.New("business failure") },
	}
	err := c.process(&Message{Topic: "source", SendDirectToDeadLetter: true})
	var failure *processError
	if !errors.As(err, &failure) || failure.Stage != "dlq" || len(failure.Stack) == 0 {
		t.Fatalf("expected recovered producer panic, got %v", err)
	}
	if !errors.Is(err, kafka.TopicAuthorizationFailed) {
		t.Fatal("recovered error cause was lost")
	}
}

func TestConsumer_BlockCommit_When_DLQDeliveryFails(t *testing.T) {
	reader := newRecoveryReader()
	var handled atomic.Int32
	c, blocked := newRecoveryConsumer(reader, 3, func(message *Message) error {
		handled.Add(1)
		if message.Offset == 7 {
			message.SendDirectToDeadLetter = true
			return errors.New("business failure")
		}
		return nil
	})
	c.deadLetterProducer = &recoveryProducer{err: kafka.TopicAuthorizationFailed}
	c.deadLetterTopic = "dead-letter"
	c.skipMessageByHeaderFn = func(headers []kafka.Header) bool { return len(headers) != 0 }
	reader.messages <- kafka.Message{Topic: "source", Offset: 7}
	reader.messages <- kafka.Message{Topic: "source", Offset: 8}
	reader.messages <- kafka.Message{Topic: "source", Offset: 9, Headers: []kafka.Header{{Key: "skip"}}}

	c.Consume()
	t.Cleanup(func() { stopRecoveryConsumer(t, c) })
	awaitRecoverySignal(t, blocked, 5*time.Second)

	if reader.commitCount() != 0 || handled.Load() != 2 {
		t.Fatalf("failed/filtered batch crossed commit fence: commits=%d handled=%d", reader.commitCount(), handled.Load())
	}
	stopRecoveryConsumer(t, c)
	if reader.commitCount() != 0 {
		t.Fatal("shutdown committed an undelivered batch")
	}
}

func TestConsumer_HeaderSkip_CommitsOnlyWithSuccessfulBatch(t *testing.T) {
	reader := newRecoveryReader()
	var handled atomic.Int32
	c, _ := newRecoveryConsumer(reader, 2, func(*Message) error { handled.Add(1); return nil })
	c.skipMessageByHeaderFn = func(headers []kafka.Header) bool { return len(headers) != 0 }
	reader.messages <- kafka.Message{Topic: "source", Offset: 7}
	reader.messages <- kafka.Message{Topic: "source", Offset: 8, Headers: []kafka.Header{{Key: "skip"}}}

	c.Consume()
	t.Cleanup(func() { stopRecoveryConsumer(t, c) })
	select {
	case committed := <-reader.commitEvents:
		if len(committed) != 2 || committed[0].Offset != 7 || committed[1].Offset != 8 || handled.Load() != 1 {
			t.Fatal("filtered messages did not share the successful batch's commit")
		}
	case <-time.After(time.Second):
		t.Fatal("successful batch was not committed")
	}
}

func TestConsumer_CommitFailure_BlocksLaterOffsets(t *testing.T) {
	reader := newRecoveryReader()
	reader.commitErr = errors.New("commit unavailable")
	c, blocked := newRecoveryConsumer(reader, 1, func(*Message) error { return nil })
	reader.messages <- kafka.Message{Topic: "source", Offset: 7}
	reader.messages <- kafka.Message{Topic: "source", Offset: 8}

	c.Consume()
	t.Cleanup(func() { stopRecoveryConsumer(t, c) })
	awaitRecoverySignal(t, blocked, time.Second)
	stopRecoveryConsumer(t, c)
	if reader.commitCount() != 1 {
		t.Fatalf("later batch was committed after a commit failure: %d calls", reader.commitCount())
	}
}

func TestConsumer_Stop_Finishes_WhenDeliveryBlocked(t *testing.T) {
	reader := newRecoveryReader()
	c, blocked := newRecoveryConsumer(reader, 1, func(*Message) error { panic("worker panic") })
	for offset := 0; offset < 100; offset++ {
		reader.messages <- kafka.Message{Topic: "source", Offset: int64(offset)}
	}

	c.Consume()
	t.Cleanup(func() { stopRecoveryConsumer(t, c) })
	awaitRecoverySignal(t, blocked, time.Second)
	stopRecoveryConsumer(t, c)
	if reader.commitCount() != 0 {
		t.Fatal("blocked worker or shutdown advanced source offsets")
	}
}

func TestConsumer_AfterRestart_CommitsPreviouslyUndeliveredMessage(t *testing.T) {
	reader := newRecoveryReader()
	c, _ := newRecoveryConsumer(reader, 1, func(message *Message) error {
		message.SendDirectToDeadLetter = true
		return errors.New("business failure")
	})
	producer := &recoveryProducer{}
	c.deadLetterProducer = producer
	reader.messages <- kafka.Message{Topic: "source", Offset: 7}
	c.Consume()
	t.Cleanup(func() { stopRecoveryConsumer(t, c) })
	select {
	case committed := <-reader.commitEvents:
		if producer.calls.Load() != 1 || len(committed) != 1 || committed[0].Offset != 7 {
			t.Fatal("source message committed before successful DLQ delivery")
		}
	case <-time.After(time.Second):
		t.Fatal("message did not complete after delivery became available")
	}
}

type recoveryProducer struct {
	err        error
	panicValue any
	calls      atomic.Int32
}

func (p *recoveryProducer) Produce(context.Context, Message) error { return p.err }
func (p *recoveryProducer) ProduceBatch(context.Context, []Message) error {
	p.calls.Add(1)
	if p.panicValue != nil {
		panic(p.panicValue)
	}
	return p.err
}
func (*recoveryProducer) Close() error { return nil }

type recoveryReader struct {
	messages     chan kafka.Message
	commitEvents chan []kafka.Message
	mu           sync.Mutex
	commits      [][]kafka.Message
	commitErr    error
}

func newRecoveryReader() *recoveryReader {
	return &recoveryReader{messages: make(chan kafka.Message, 128), commitEvents: make(chan []kafka.Message, 128)}
}
func (r *recoveryReader) FetchMessage(ctx context.Context, message *kafka.Message) error {
	select {
	case msg := <-r.messages:
		*message = msg
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *recoveryReader) CommitMessages(messages []kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := append([]kafka.Message(nil), messages...)
	r.commits = append(r.commits, copy)
	if r.commitErr == nil {
		r.commitEvents <- copy
	}
	return r.commitErr
}
func (r *recoveryReader) commitCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.commits)
}
func (*recoveryReader) Close() error { return nil }

type recoveryLogger struct {
	LoggerInterface
	blocked chan struct{}
	once    sync.Once
}

func (l *recoveryLogger) Errorf(format string, args ...interface{}) {
	l.LoggerInterface.Errorf(format, args...)
	if strings.Contains(format, "Consumer delivery blocked") {
		l.once.Do(func() { close(l.blocked) })
	}
}

func newRecoveryConsumer(reader *recoveryReader, concurrency int, consumeFn func(*Message) error) (*consumer, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	log := &recoveryLogger{LoggerInterface: NewZapLogger(LogLevelError), blocked: make(chan struct{})}
	c := &consumer{base: &base{
		r: reader, context: ctx, cancelFn: cancel, logger: log, metric: &ConsumerMetric{},
		consumerCfg: &ConsumerConfig{}, consumerState: stateRunning,
		incomingMessageStream: make(chan *IncomingMessage, concurrency),
		quit:                  make(chan struct{}), pause: make(chan struct{}), concurrency: concurrency,
		messageGroupDuration: time.Millisecond, deferHeaderCommit: true,
	}, consumeFn: consumeFn}
	c.initializeStreams()
	return c, log.blocked
}

func awaitRecoverySignal(t *testing.T, signal <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(timeout):
		t.Fatal("consumer did not reach expected state")
	}
}
func stopRecoveryConsumer(t *testing.T, c *consumer) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("stop consumer: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("consumer shutdown blocked")
	}
}
