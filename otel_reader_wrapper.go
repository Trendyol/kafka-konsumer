package kafka

import (
	"context"

	"github.com/Trendyol/otel-kafka-konsumer"
	segmentio "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
)

type otelReaderWrapper struct {
	r *otelkafkakonsumer.Reader
}

func NewOtelReaderWrapper(cfg *ConsumerConfig, reader *segmentio.Reader) (Reader, error) {
	cfg.setDefaults()

	newReader, err := otelkafkakonsumer.NewReader(
		reader,
		otelkafkakonsumer.WithTracerProvider(cfg.DistributedTracingConfiguration.TracerProvider),
		otelkafkakonsumer.WithPropagator(cfg.DistributedTracingConfiguration.Propagator),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String(cfg.Reader.GroupID),
			},
		))
	if err != nil {
		return nil, err
	}

	return &otelReaderWrapper{
		r: newReader,
	}, nil
}

func (o *otelReaderWrapper) FetchMessage(ctx context.Context, msg *segmentio.Message) error {
	return o.r.FetchMessage(ctx, msg)
}

func (o *otelReaderWrapper) Close() error {
	return o.r.Close()
}

func (o *otelReaderWrapper) CommitMessages(messages []segmentio.Message) error {
	return o.r.CommitMessages(context.Background(), messages...)
}
