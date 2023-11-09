package main

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
	"os"
	"os/signal"
	"time"

	"github.com/Trendyol/kafka-konsumer"
)

func main() {
	jaegerUrl := "http://localhost:14268/api/traces"
	tp := initJaegerTracer(jaegerUrl)
	defer tp.Shutdown(context.Background())

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// ===============SIMULATE PRODUCER===============
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{
			Brokers: []string{"localhost:29092"},
		},
		DistributedTracingEnabled: true,
	})

	const topicName = "standart-topic"
	producedMessage := kafka.Message{
		Topic: topicName,
		Key:   []byte("1"),
		Value: []byte(`{ "foo": "bar" }`),
	}

	tr := otel.Tracer("after producing")
	parentCtx, span := tr.Start(context.Background(), "before producing work")
	time.Sleep(100 * time.Millisecond)
	span.End()

	_ = producer.Produce(parentCtx, producedMessage)

	// ===============SIMULATE CONSUMER===============
	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: []string{"localhost:29092"},
			Topic:   topicName,
			GroupID: "standart-cg",
		},
		ConsumeFn:                 consumeFn,
		DistributedTracingEnabled: true,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func consumeFn(message *kafka.Message) error {
	fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))

	tr := otel.Tracer("consumer")
	parentCtx, span := tr.Start(message.Context, "work")
	time.Sleep(100 * time.Millisecond)
	span.End()

	_, span = tr.Start(parentCtx, "another work")
	time.Sleep(50 * time.Millisecond)
	span.End()

	return nil
}

func initJaegerTracer(url string) *trace.TracerProvider {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		panic("Err initializing jaeger instance" + err.Error())
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("kafka-konsumer-demo"),
			attribute.String("environment", "prod"),
		)),
	)

	return tp
}
