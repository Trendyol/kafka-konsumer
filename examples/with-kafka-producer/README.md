### Kafka Producer

This example demonstrates how to use the built-in `kafka-konsumer` producer to publish single
messages and batches of messages to a Kafka topic.

### Prerequisites

- Go 1.21+
- A running Kafka broker (see [docker-compose.yml](../docker-compose.yml))

### How to run demo?

Start the Kafka broker:

```sh
docker-compose up -d
```

Run the producer:

```sh
go run main.go
```

The program produces one individual message and a batch of two messages to `standart-topic`, then exits.
You can verify the messages were delivered:

```sh
kafka-console-consumer --bootstrap-server localhost:29092 --topic standart-topic --from-beginning
```

### API

**Produce a single message:**

```go
_ = producer.Produce(context.Background(), kafka.Message{
    Topic: "my-topic",
    Key:   []byte("key"),
    Value: []byte(`{"hello": "world"}`),
})
```

**Produce a batch of messages:**

```go
_ = producer.ProduceBatch(context.Background(), []kafka.Message{
    {Topic: "my-topic", Key: []byte("1"), Value: []byte(`{"a": 1}`)},
    {Topic: "my-topic", Key: []byte("2"), Value: []byte(`{"a": 2}`)},
})
```

### Limitation

This example omits error handling for brevity. In production code, always check the error returned
by `Produce` and `ProduceBatch` and implement appropriate retry or alerting logic.
