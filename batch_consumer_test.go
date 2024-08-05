package kafka

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	lcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
)

func Test_batchConsumer_startBatch(t *testing.T) {
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
			messageGroupDuration:   500 * time.Millisecond,
			r:                      &mc,
			concurrency:            1,
		},
		messageGroupLimit: 3,
		consumeFn: func(_ []*Message) error {
			numberOfBatch++
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
	if numberOfBatch != 2 {
		t.Fatalf("Number of batch group must equal to 2")
	}
	if bc.metric.TotalProcessedMessagesCounter != 4 {
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
	if bc.metric.TotalProcessedMessagesCounter != 2 {
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
		if bc.metric.TotalProcessedMessagesCounter != 3 {
			t.Fatalf("Total Processed Message Counter must equal to 3")
		}
		if bc.metric.TotalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
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
		if bc.metric.TotalProcessedMessagesCounter != 3 {
			t.Fatalf("Total Processed Message Counter must equal to 3")
		}
		if bc.metric.TotalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Disabled", func(t *testing.T) {
		// Given
		bc := batchConsumer{
			base: &base{metric: &ConsumerMetric{}, transactionalRetry: true, logger: NewZapLogger(LogLevelDebug)},
			consumeFn: func(messages []*Message) error {
				return errors.New("error case")
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if bc.metric.TotalUnprocessedMessagesCounter != 3 {
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
			consumeFn: func(messages []*Message) error {
				return errors.New("error case")
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if bc.metric.TotalUnprocessedMessagesCounter != 3 {
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
			consumeFn: func(messages []*Message) error {
				return errors.New("error case")
			},
		}

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if bc.metric.TotalUnprocessedMessagesCounter != 3 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 3")
		}
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

		// When
		bc.process([]*Message{{}, {}, {}})

		// Then
		if bc.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if bc.metric.TotalUnprocessedMessagesCounter != 3 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 3")
		}
	})
}

func Test_batchConsumer_chunk(t *testing.T) {
	tests := []struct {
		allMessages   []*Message
		expected      [][]*Message
		chunkSize     int
		chunkByteSize int
	}{
		{
			allMessages:   createMessages(0, 9),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 3),
				createMessages(3, 6),
				createMessages(6, 9),
			},
		},
		{
			allMessages:   []*Message{},
			chunkSize:     3,
			chunkByteSize: 10000,
			expected:      [][]*Message{},
		},
		{
			allMessages:   createMessages(0, 1),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 1),
			},
		},
		{
			allMessages:   createMessages(0, 8),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 3),
				createMessages(3, 6),
				createMessages(6, 8),
			},
		},
		{
			allMessages:   createMessages(0, 3),
			chunkSize:     3,
			chunkByteSize: 10000,
			expected: [][]*Message{
				createMessages(0, 3),
			},
		},

		{
			allMessages:   createMessages(0, 3),
			chunkSize:     100,
			chunkByteSize: 4,
			expected: [][]*Message{
				createMessages(0, 1),
				createMessages(1, 2),
				createMessages(2, 3),
			},
		},
	}

	for i, tc := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			chunkedMessages := chunkMessages(&tc.allMessages, tc.chunkSize, tc.chunkByteSize)

			if !reflect.DeepEqual(chunkedMessages, tc.expected) && !(len(chunkedMessages) == 0 && len(tc.expected) == 0) {
				t.Errorf("For chunkSize %d, expected %v, but got %v", tc.chunkSize, tc.expected, chunkedMessages)
			}
		})
	}
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
		bc := batchConsumer{consumeFn: func(messages []*Message) error {
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

type mockCronsumer struct {
	wantErr bool
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
	if m.wantErr {
		return errors.New("error")
	}
	return nil
}

func (m *mockCronsumer) GetMetricCollectors() []prometheus.Collector {
	panic("implement me")
}

func (m *mockCronsumer) ProduceBatch([]kcronsumer.Message) error {
	if m.wantErr {
		return errors.New("error")
	}
	return nil
}
