package kafka

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	lcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
)

func Test_batchConsumer_startBatch(t *testing.T) {
	// Given
	var numberOfBatch atomic.Int64

	mc := mockReader{}
	bc := batchConsumer{
		base: &base{
			incomingMessageStream:  make(chan *IncomingMessage, 1),
			batchConsumingStream:   make(chan []*Message, 1),
			singleConsumingStream:  make(chan *Message, 1),
			messageProcessedStream: make(chan struct{}, 1),
			metric:                 &ConsumerMetric{},
			wg:                     sync.WaitGroup{},
			messageGroupDuration:   500 * time.Millisecond,
			r:                      &mc,
			concurrency:            1,
			logger:                 NewZapLogger(LogLevelDebug),
		},
		messageGroupLimit: 3,
		consumeFn: func(_ []*Message) error {
			numberOfBatch.Add(1)
			return nil
		},
	}
	go func() {
		// Simulate messageGroupLimit
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Simulate messageGroupDuration
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Return from startBatch
		close(bc.base.incomingMessageStream)
	}()

	bc.base.wg.Add(1 + bc.base.concurrency)

	// When
	bc.setupConcurrentWorkers()
	bc.startBatch()

	// Then
	if numberOfBatch.Load() != 2 {
		t.Fatalf("Number of batch group must equal to 2")
	}
	if bc.metric.totalProcessedMessagesCounter != 4 {
		t.Fatalf("Total Processed Message Counter must equal to 4")
	}
}

func Test_batchConsumer_startBatch_with_preBatch(t *testing.T) {
	// Given
	var numberOfBatch int

	mc := mockReader{}
	bc := batchConsumer{
		base: &base{
			incomingMessageStream:  make(chan *IncomingMessage, 1),
			batchConsumingStream:   make(chan []*Message, 1),
			singleConsumingStream:  make(chan *Message, 1),
			messageProcessedStream: make(chan struct{}, 1),
			metric:                 &ConsumerMetric{},
			wg:                     sync.WaitGroup{},
			messageGroupDuration:   20 * time.Second,
			r:                      &mc,
			concurrency:            1,
			logger:                 NewZapLogger(LogLevelDebug),
		},
		messageGroupLimit: 2,
		consumeFn: func(_ []*Message) error {
			numberOfBatch++
			return nil
		},
		preBatchFn: func(messages []*Message) []*Message {
			return messages[:1]
		},
	}
	go func() {
		// Simulate messageGroupLimit
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Simulate messageGroupDuration
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		bc.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Return from startBatch
		close(bc.base.incomingMessageStream)
	}()

	bc.base.wg.Add(1 + bc.base.concurrency)

	// When
	bc.setupConcurrentWorkers()
	bc.startBatch()

	// Then
	if numberOfBatch != 2 {
		t.Fatalf("Number of batch group must equal to 2")
	}
	if bc.metric.totalProcessedMessagesCounter != 2 {
		t.Fatalf("Total Processed Message Counter must equal to 2")
	}
}

func Test_batchConsumer_process(t *testing.T) {
	t.Run("When_Processing_Is_Successful", func(t *testing.T) {
		// Given
		bc := batchConsumer{
			base: &base{metric: &ConsumerMetric{}, transactionalRetry: true},
			consumeFn: func([]*Message) error {
				return nil
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.totalProcessedMessagesCounter != 3 {
			t.Fatalf("Total Processed Message Counter must equal to 3")
		}
		if bc.metric.totalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Processing_Is_Partial_Successful_And_Transactional_Retry_Disabled", func(t *testing.T) {
		// Given
		bc := batchConsumer{
			base: &base{metric: &ConsumerMetric{}, transactionalRetry: false},
			consumeFn: func(messages []*Message) error {
				messages[0].IsFailed = true
				messages[1].IsFailed = false
				messages[2].IsFailed = true

				return errors.New("error case")
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.totalUnprocessedMessagesCounter != 2 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 2")
		}
		if bc.metric.totalProcessedMessagesCounter != 1 {
			t.Fatalf("Total Processed Message Counter must equal to 1")
		}
	})
	t.Run("When_Re-processing_Is_Successful", func(t *testing.T) {
		// Given
		gotOnlyOneTimeException := true
		bc := batchConsumer{
			base: &base{metric: &ConsumerMetric{}, transactionalRetry: true, logger: NewZapLogger(LogLevelDebug)},
			consumeFn: func(_ []*Message) error {
				if gotOnlyOneTimeException {
					gotOnlyOneTimeException = false
					return errors.New("simulate only one time exception")
				}
				return nil
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.totalProcessedMessagesCounter != 3 {
			t.Fatalf("Total Processed Message Counter must equal to 3")
		}
		if bc.metric.totalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Disabled", func(t *testing.T) {
		// Given
		bc := batchConsumer{
			base: &base{metric: &ConsumerMetric{}, transactionalRetry: true, logger: NewZapLogger(LogLevelDebug)},
			consumeFn: func(_ []*Message) error {
				return errors.New("error case")
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.totalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if bc.metric.totalUnprocessedMessagesCounter != 3 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 3")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Enabled", func(t *testing.T) {
		// Given
		mc := mockCronsumer{}
		bc := batchConsumer{
			base: &base{
				metric: &ConsumerMetric{}, transactionalRetry: true,
				logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc,
			},
			consumeFn: func(_ []*Message) error {
				return errors.New("error case")
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.totalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if bc.metric.totalUnprocessedMessagesCounter != 3 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 3")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Failed", func(t *testing.T) {
		// Given
		mc := mockCronsumer{wantErr: true}
		bc := batchConsumer{
			base: &base{
				metric: &ConsumerMetric{}, transactionalRetry: true,
				logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc,
			},
			consumeFn: func(_ []*Message) error {
				return errors.New("error case")
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()

		// When && Then
		bc.process([]*Message{{}, {}, {}})
	})

	t.Run("When_Re-processing_Is_Failed_And_Retry_Failed_5_times", func(t *testing.T) {
		// Given
		mc := mockCronsumer{wantErr: true, retryBehaviorOpen: true, maxRetry: 5}
		bc := batchConsumer{
			base: &base{
				metric: &ConsumerMetric{}, transactionalRetry: true,
				logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc,
			},
			consumeFn: func(_ []*Message) error {
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
		bc.process([]*Message{{}, {}, {}})
	})

	t.Run("When_Transactional_Retry_Disabled", func(t *testing.T) {
		// Given
		mc := &mockCronsumer{wantErr: true}
		bc := batchConsumer{
			base: &base{
				metric:             &ConsumerMetric{},
				logger:             NewZapLogger(LogLevelDebug),
				retryEnabled:       true,
				transactionalRetry: false,
				cronsumer:          mc,
			},
			consumeFn: func(messages []*Message) error {
				messages[0].IsFailed = true
				messages[1].IsFailed = true

				return errors.New("error case")
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()

		// When && Then
		bc.process([]*Message{{}, {}, {}})
	})

	t.Run("When_SendDirectToDeadLetter_True_And_RetryEnabled_Should_Not_Call_Retry", func(t *testing.T) {
		// Given
		mdlp := &mockDeadLetterProducer{}
		mc := mockCronsumer{wantErr: true, retryBehaviorOpen: true, maxRetry: 5}
		bc := batchConsumer{
			base: &base{
				metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: mdlp,
				retryEnabled: true, cronsumer: &mc,
			},
			consumeFn: func(_ []*Message) error { return errors.New("err occurred") },
		}
		msgs := []*Message{
			{Key: []byte("1"), Value: []byte("foo"), SendDirectToDeadLetter: true},
			{Key: []byte("2"), Value: []byte("bar"), SendDirectToDeadLetter: true},
		}

		// When
		bc.process(msgs)

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
		bc := batchConsumer{
			base:      &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: mdlp},
			consumeFn: func(_ []*Message) error { return errors.New("err occurred") },
		}
		msgs := []*Message{{Key: []byte("1"), Value: []byte("foo"), SendDirectToDeadLetter: true}}

		// When
		bc.process(msgs)

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
		if bc.metric.totalUnprocessedMessagesCounter != 1 {
			t.Fatalf("totalUnprocessedMessagesCounter must be 1, got %d", bc.metric.totalUnprocessedMessagesCounter)
		}
		if bc.metric.totalProcessedMessagesCounter != 0 {
			t.Fatalf("totalProcessedMessagesCounter must be 0, got %d", bc.metric.totalProcessedMessagesCounter)
		}
	})

	t.Run("When_SendDirectToDeadLetter_True_Should_Use_ErrDescription_In_Header", func(t *testing.T) {
		// Given
		mdlp := &mockDeadLetterProducer{}
		bc := batchConsumer{
			base:      &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: mdlp},
			consumeFn: func(_ []*Message) error { return errors.New("ignored by ErrDescription") },
		}
		msgs := []*Message{{Key: []byte("2"), Value: []byte("bar"), SendDirectToDeadLetter: true, ErrDescription: "custom direct error"}}

		// When
		bc.process(msgs)

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
		bc := batchConsumer{
			base:      &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), deadLetterProducer: fdlp},
			consumeFn: func(_ []*Message) error { return errors.New("err occurred") },
		}
		msgs := []*Message{{Key: []byte("1"), Value: []byte("foo"), SendDirectToDeadLetter: true}}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
			if fdlp.called != 5 {
				t.Fatalf("dead letter producer must be called 5 times with backoff, got %d", fdlp.called)
			}
		}()

		// When && Then
		bc.process(msgs)
	})
}

func Test_batchConsumer_Pause(t *testing.T) {
	// Given
	ctx, cancelFn := context.WithCancel(context.Background())
	bc := batchConsumer{
		base: &base{
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			context: ctx, cancelFn: cancelFn,
			consumerState: stateRunning,
		},
	}

	go func() {
		<-bc.base.pause
	}()

	// When
	bc.Pause()

	// Then
	if bc.base.consumerState != statePaused {
		t.Fatal("consumer state must be in paused")
	}
}

func Test_batchConsumer_Resume(t *testing.T) {
	// Given
	mc := mockReader{}
	ctx, cancelFn := context.WithCancel(context.Background())
	bc := batchConsumer{
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
	bc.Resume()

	// Then
	if bc.base.consumerState != stateRunning {
		t.Fatal("consumer state must be in resume!")
	}
}

func Test_batchConsumer_runKonsumerFn(t *testing.T) {
	t.Run("Should_Return_Default_Error_When_Error_Description_Does_Not_Exist", func(t *testing.T) {
		// Given
		expectedError := errors.New("default error")
		bc := batchConsumer{consumeFn: func(_ []*Message) error {
			return expectedError
		}}

		// When
		actualError := bc.runKonsumerFn(kcronsumer.Message{})

		// Then
		if actualError.Error() != expectedError.Error() {
			t.Fatalf("actual error = %s should be equal to expected error = %s", actualError.Error(), expectedError.Error())
		}
	})

	t.Run("Should_Return_Message_Error_Description_When_Error_Description_Exist", func(t *testing.T) {
		// Given
		expectedError := errors.New("message error description")
		bc := batchConsumer{consumeFn: func(messages []*Message) error {
			messages[0].ErrDescription = "message error description"
			return errors.New("default error")
		}}

		// When
		actualError := bc.runKonsumerFn(kcronsumer.Message{})

		// Then
		if actualError.Error() != expectedError.Error() {
			t.Fatalf("actual error = %s should be equal to expected error = %s", actualError.Error(), expectedError.Error())
		}
	})
}

func Test_batchConsumer_chunk(t *testing.T) {
	type testCase struct {
		name          string
		allMessages   []*Message
		chunkSize     int
		chunkByteSize int
		expected      [][]*Message
		shouldPanic   bool
	}

	tests := []testCase{
		{
			name:          "Should_Return_3_Chunks_For_9_Messages",
			allMessages:   createMessages(0, 9),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 3),
				createMessages(3, 6),
				createMessages(6, 9),
			},
			shouldPanic: false,
		},
		{
			name:          "Should_Return_Empty_Slice_When_Input_Is_Empty",
			allMessages:   []*Message{},
			chunkSize:     3,
			chunkByteSize: 10000,
			expected:      [][]*Message{},
			shouldPanic:   false,
		},
		{
			name:          "Should_Return_Single_Chunk_When_Single_Message",
			allMessages:   createMessages(0, 1),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 1),
			},
			shouldPanic: false,
		},
		{
			name:          "Should_Splits_Into_Multiple_Chunks_With_Incomplete_Final_Chunk",
			allMessages:   createMessages(0, 8),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 3),
				createMessages(3, 6),
				createMessages(6, 8),
			},
			shouldPanic: false,
		},
		{
			name:          "Should_Return_Exact_Chunk_Size_Forms_Single_Chunk",
			allMessages:   createMessages(0, 3),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 3),
			},
			shouldPanic: false,
		},
		{
			name:          "Should_Forces_Single_Message_Per_Chunk_When_Small_chunkByteSize_Is_Given",
			allMessages:   createMessages(0, 3),
			chunkSize:     100,
			chunkByteSize: 4, // Each message has Value size 4
			expected: [][]*Message{
				createMessages(0, 1),
				createMessages(1, 2),
				createMessages(2, 3),
			},
			shouldPanic: false,
		},
		{
			name:          "Should_Ignore_Byte_Size_When_chunkByteSize=0",
			allMessages:   createMessages(0, 5),
			chunkSize:     2,
			chunkByteSize: 0,
			expected: [][]*Message{
				createMessages(0, 2),
				createMessages(2, 4),
				createMessages(4, 5),
			},
			shouldPanic: false,
		},
		{
			name:          "Should_Panic_When_chunkByteSize_Less_Than_Message_Size",
			allMessages:   createMessages(0, 1),
			chunkSize:     2,
			chunkByteSize: 3, // Message size is 4
			expected:      nil,
			shouldPanic:   true,
		},
		{
			name:          "Should_Panic_When_chunkSize=0",
			allMessages:   createMessages(0, 1),
			chunkSize:     0,
			chunkByteSize: 10000,
			expected:      nil,
			shouldPanic:   true,
		},
		{
			name:          "Should_Panic_When_Negative_chunkSize",
			allMessages:   createMessages(0, 1),
			chunkSize:     -1,
			chunkByteSize: 10000,
			expected:      nil,
			shouldPanic:   true,
		},
		{
			name:          "Should_Return_Exact_chunkByteSize",
			allMessages:   createMessages(0, 4),
			chunkSize:     2,
			chunkByteSize: 8, // Each message has Value size 4, total 16 bytes
			expected: [][]*Message{
				createMessages(0, 2),
				createMessages(2, 4),
			},
			shouldPanic: false,
		},
		{
			name: "Should_Handle_Varying_Message_Byte_Sizes",
			allMessages: []*Message{
				{Partition: 0, Value: []byte("a")},    // 1 byte
				{Partition: 1, Value: []byte("ab")},   // 2 bytes
				{Partition: 2, Value: []byte("abc")},  // 3 bytes
				{Partition: 3, Value: []byte("abcd")}, // 4 bytes
			},
			chunkSize:     3,
			chunkByteSize: 6,
			expected: [][]*Message{
				{
					{Partition: 0, Value: []byte("a")},
					{Partition: 1, Value: []byte("ab")},
					{Partition: 2, Value: []byte("abc")},
				},
				{
					{Partition: 3, Value: []byte("abcd")},
				},
			},
			shouldPanic: false,
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			if tc.shouldPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for test case '%s', but did not panic", tc.name)
					}
				}()
			}

			chunkedMessages := chunkMessagesOptimized(tc.allMessages, tc.chunkSize, tc.chunkByteSize)

			if !tc.shouldPanic {
				// Verify the number of chunks
				if len(chunkedMessages) != len(tc.expected) {
					t.Errorf("Test case '%s': expected %d chunks, got %d", tc.name, len(tc.expected), len(chunkedMessages))
				}

				// Verify each chunk's content
				for i, expectedChunk := range tc.expected {
					if i >= len(chunkedMessages) {
						t.Errorf("Test case '%s': missing chunk %d", tc.name, i)
						continue
					}
					actualChunk := chunkedMessages[i]
					if !messagesEqual(actualChunk, expectedChunk) {
						t.Errorf("Test case '%s': expected chunk %d to be %v, but got %v", tc.name, i, expectedChunk, actualChunk)
					}
				}
			}
		})
	}
}

func createMessages(partitionStart int, partitionEnd int) []*Message {
	messages := make([]*Message, 0)
	for i := partitionStart; i < partitionEnd; i++ {
		messages = append(messages, &Message{
			Partition: i,
			Value:     []byte("test"),
		})
	}
	return messages
}

func messagesEqual(a, b []*Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Partition != b[i].Partition {
			return false
		}
		if !reflect.DeepEqual(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

type mockCronsumer struct {
	wantErr           bool
	retryBehaviorOpen bool
	times             int
	maxRetry          int
}

func (m *mockCronsumer) Start() {
	panic("implement me")
}

func (m *mockCronsumer) Run() {
	panic("implement me")
}

func (m *mockCronsumer) Stop() {
	panic("implement me")
}

func (m *mockCronsumer) WithLogger(_ lcronsumer.Interface) {
	panic("implement me")
}

func (m *mockCronsumer) Produce(_ kcronsumer.Message) error {
	if m.retryBehaviorOpen {
		if m.wantErr && m.times <= m.maxRetry {
			m.times++
			return errors.New("error")
		}
		return nil
	}
	if m.wantErr {
		return errors.New("error")
	}
	return nil
}

func (m *mockCronsumer) GetMetricCollectors() []prometheus.Collector {
	panic("implement me")
}

func (m *mockCronsumer) ProduceBatch([]kcronsumer.Message) error {
	if m.retryBehaviorOpen {
		if m.wantErr && m.times <= m.maxRetry {
			m.times++
			return errors.New("error")
		}
		return nil
	}
	if m.wantErr {
		return errors.New("error")
	}
	return nil
}
