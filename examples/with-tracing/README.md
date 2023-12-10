### Distributed Tracing

[segmentio/kafka-go](https://github.com/segmentio/kafka-go) has no support for open telemetry. 
There is also an [issue](https://github.com/segmentio/kafka-go/issues/1025) about it.
Based on some work on that issue, we created a project called [otel-kafka-konsumer](https://github.com/Trendyol/otel-kafka-konsumer).

By integrating this project with kafka-konsumer, we successfully implemented distributed tracing in consuming
and producing operations. You can run demo. 

In this demo, we chose jaeger to show how to integrate distributed tracing on your project using kafka-konsumer. 
But zipkin, stdout, and other alternatives are still applicable

Two settings are significant.
- trace.TracerProvider _(you can set jaeger,zipkin etc.)_
- propagation.TextMapPropagator (please refer to [here](https://opentelemetry.io/docs/specs/otel/context/api-propagators/))

If you have not specified its values, kafka-konsumer uses global.TraceProvider and Propagation.

### Demo overview

![Tracing Example](../../.github/images/tracing.png)

### How to run demo?

You should run [docker-compose.yml](../docker-compose.yml) by

```sh
docker-compose up build
```

You can access the jaeger dashboard as [jaeger dashboard](http://localhost:16686/search)

You can run the demo as `go run main.go`

```go
package main

import (
	"context"
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
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
```

In the producing step, we open only two spans. In the consuming step, we open three spans. You can see their relationship via the jeager dashboard, as shown below.

![Demo Jeager](../../.github/images/jaeger-dashboard-example.png)