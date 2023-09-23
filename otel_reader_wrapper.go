package kafka

import (
	"context"

	"github.com/Abdulsametileri/otel-kafka-konsumer"
	segmentio "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
)

type otelReaderWrapper struct {
	r *otelkafkakonsumer.Reader
}

func NewOtelReaderWrapper(cfg *ConsumerConfig, reader *segmentio.Reader) (Reader, error) {
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

func (o *otelReaderWrapper) ReadMessage(ctx context.Context) (*segmentio.Message, error) {
	return o.r.ReadMessage(ctx)
}

func (o *otelReaderWrapper) Close() error {
	return o.r.Close()
}
