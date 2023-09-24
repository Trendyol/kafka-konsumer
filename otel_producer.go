package kafka

import (
	"context"
	"fmt"

	"github.com/Abdulsametileri/otel-kafka-konsumer"
	segmentio "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
)

type otelProducer struct {
	w *otelkafkakonsumer.Writer
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

// TODO: we need to take care of batch producing performance
func (o *otelProducer) WriteMessages(ctx context.Context, messages ...segmentio.Message) error {
	for i := range messages {
		if err := o.w.WriteMessages(ctx, messages[i]); err != nil {
			return fmt.Errorf("error during producing %w", err)
		}
	}
	return nil
}

func (o *otelProducer) Close() error {
	return o.w.Close()
}
