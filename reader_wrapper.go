package kafka

import (
	"context"

	segmentio "github.com/segmentio/kafka-go"
)

type readerWrapper struct {
	r *segmentio.Reader
}

func NewReaderWrapper(reader *segmentio.Reader) Reader {
	return &readerWrapper{r: reader}
}

// ReadMessage returns pointer of kafka message because we will support distributed tracing in the near future
func (s *readerWrapper) FetchMessage(ctx context.Context) (*segmentio.Message, error) {
	message, err := s.r.FetchMessage(ctx)
	return &message, err
}

func (s *readerWrapper) Close() error {
	return s.r.Close()
}

func (s *readerWrapper) CommitMessages(messages []segmentio.Message) error {
	return s.r.CommitMessages(context.Background(), messages...)
}
