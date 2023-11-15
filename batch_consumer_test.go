package kafka

import (
	"errors"
	"sync"
	"testing"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	lcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
)

func Test_batchConsumer_startBatch(t *testing.T) {
	// Given
	var numberOfBatch int

	bc := batchConsumer{
		base: &base{
			messageCh: make(chan *Message),
			metric:    &ConsumerMetric{},
			wg:        sync.WaitGroup{},
		},
		messageGroupLimit:    3,
		messageGroupDuration: 500 * time.Millisecond,
		consumeFn: func(messages []*Message) error {
			numberOfBatch++
			return nil
		},
	}
	go func() {
		// Simulate messageGroupLimit
		bc.base.messageCh <- &Message{}
		bc.base.messageCh <- &Message{}
		bc.base.messageCh <- &Message{}

		time.Sleep(1 * time.Second)

		// Simulate messageGroupDuration
		bc.base.messageCh <- &Message{}

		time.Sleep(1 * time.Second)

		// Return from startBatch
		close(bc.base.messageCh)
	}()

	bc.base.wg.Add(1)

	// When
	bc.startBatch()

	// Then
	if numberOfBatch != 2 {
		t.Fatalf("Number of batch group must equal to 2")
	}
	if bc.metric.TotalProcessedMessagesCounter != 4 {
		t.Fatalf("Total Processed Message Counter must equal to 4")
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
			consumeFn: func(messages []*Message) error {
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

func (m *mockCronsumer) WithLogger(logger lcronsumer.Interface) {
	panic("implement me")
}

func (m *mockCronsumer) Produce(message kcronsumer.Message) error {
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
