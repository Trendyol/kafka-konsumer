package kafka

import (
	"context"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

type retentionTestReader struct {
	mockReader
	checkCommit func([]kafka.Message)
}

type retentionContextKey struct{}

func (r *retentionTestReader) CommitMessages(messages []kafka.Message) error {
	r.checkCommit(messages)
	return r.mockReader.CommitMessages(messages)
}

func Test_consume_releasesCompletedReferences(t *testing.T) {
	const batchKind = "batch"
	for _, kind := range []string{"single", batchKind} {
		for _, failCommit := range []bool{false, true} {
			name := kind + "/commit-success"
			if failCommit {
				name = kind + "/commit-error"
			}
			t.Run(name, func(t *testing.T) {
				const capacity = 4
				messages := make([]*Message, 0, capacity)
				commits := make([]kafka.Message, 0, capacity)
				committed := 0
				reader := &retentionTestReader{
					mockReader: mockReader{wantErr: failCommit},
					checkCommit: func(batch []kafka.Message) {
						for _, message := range batch {
							if string(message.Value) != "payload" || len(message.Headers) != 1 {
								t.Error("message cleared before commit returned")
							}
						}
						committed += len(batch)
					},
				}
				b := &base{
					r:                      reader,
					logger:                 NewZapLogger(LogLevelError),
					singleConsumingStream:  make(chan *Message),
					batchConsumingStream:   make(chan []*Message),
					messageProcessedStream: make(chan struct{}, capacity),
				}
				// Reusing the buffer for a smaller batch must not retain the old tail.
				for _, count := range []int{capacity, 1} {
					for i := 0; i < count; i++ {
						message := kafka.Message{
							Value:   []byte("payload"),
							Headers: []kafka.Header{{Key: "baggage", Value: []byte("key=value")}},
						}
						m := fromKafkaMessage(&message)
						m.Context = context.WithValue(context.Background(), retentionContextKey{}, []byte("trace-data"))
						messages = append(messages, m)
						commits = append(commits, message)
					}
					workerDone := make(chan struct{})
					go func() {
						defer close(workerDone)
						check := func(m *Message) {
							if string(m.Value) != "payload" || m.Context == nil {
								t.Error("message cleared before handler completed")
							}
						}
						if kind == batchKind {
							for _, m := range <-b.batchConsumingStream {
								check(m)
							}
							b.messageProcessedStream <- struct{}{}
						} else {
							for i := 0; i < count; i++ {
								check(<-b.singleConsumingStream)
								b.messageProcessedStream <- struct{}{}
							}
						}
					}()
					if kind == batchKind {
						byteCount := count * len("payload")
						c := &batchConsumer{base: b, messageGroupLimit: capacity}
						c.consume(&messages, &commits, &byteCount)
						if byteCount != 0 {
							t.Error("batch byte count was not reset")
						}
					} else {
						c := &consumer{base: b}
						c.consume(&messages, &commits)
					}
					<-workerDone
					if len(messages) != 0 || len(commits) != 0 || cap(messages) != capacity || cap(commits) != capacity {
						t.Fatal("cleanup changed buffer capacity or failed to reset length")
					}
					for i, m := range messages[:cap(messages)] {
						if m != nil {
							t.Errorf("completed message/context retained at slot %d", i)
						}
					}
					for i, m := range commits[:cap(commits)] {
						if !reflect.DeepEqual(m, kafka.Message{}) {
							t.Errorf("completed commit payload/header retained at slot %d", i)
						}
					}
				}
				if committed != capacity+1 {
					t.Errorf("expected %d committed messages, got %d", capacity+1, committed)
				}
			})
		}
	}
}
