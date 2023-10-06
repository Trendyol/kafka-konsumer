package kafka

import (
	"context"
	"fmt"

	"github.com/Trendyol/otel-kafka-konsumer"
	segmentio "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
)

type OtelKafkaKonsumerWriter interface {
	WriteMessage(ctx context.Context, msg segmentio.Message) error
	WriteMessages(ctx context.Context, msgs []segmentio.Message) error
	Close() error
}

type otelProducer struct {
	w OtelKafkaKonsumerWriter
}

func NewOtelProducer(cfg *ProducerConfig, writer *segmentio.Writer) (Writer, error) {
	cfg.setDefaults()

	w, err := otelkafkakonsumer.NewWriter(writer,
		otelkafkakonsumer.WithTracerProvider(cfg.DistributedTracingConfiguration.TracerProvider),
		otelkafkakonsumer.WithPropagator(cfg.DistributedTracingConfiguration.Propagator),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String(cfg.ClientID),
			},
		))
	if err != nil {
		return nil, err
	}

	return &otelProducer{
		w: w,
	}, nil
}

// Currently, we are not support tracing on batch producing. You can create custom span.
// There is an issue about it: https://github.com/Trendyol/otel-kafka-konsumer/issues/4
func (o *otelProducer) WriteMessages(ctx context.Context, messages ...segmentio.Message) error {
	if len(messages) == 1 {
		if err := o.w.WriteMessage(ctx, messages[0]); err != nil {
			return fmt.Errorf("error during producing %w", err)
		}
		return nil
	}

	if err := o.w.WriteMessages(ctx, messages); err != nil {
		return fmt.Errorf("error during batch producing %w", err)
	}

	return nil
}

func (o *otelProducer) Close() error {
	return o.w.Close()
}
