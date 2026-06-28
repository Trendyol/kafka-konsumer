### Batch Consumer

This example demonstrates how to consume messages in batches using `kafka-konsumer`.
Instead of processing one message at a time, the batch consumer collects up to
`MessageGroupLimit` messages within a `MessageGroupDuration` window and passes them
all at once to `BatchConsumeFn`. This pattern is well-suited for bulk database writes,
aggregation, and other workloads that benefit from processing multiple records together.

### Prerequisites

- Go 1.21+
- A running Kafka broker (see [docker-compose.yml](../docker-compose.yml))

### How to run demo?

Start the Kafka broker:

```sh
docker-compose up -d
```

Load the topic with sample data using the provided load file:

```sh
kafka-console-producer --broker-list localhost:29092 --topic standart-topic < ../load.txt
```

Run the batch consumer:

```sh
go run main.go
```

You will see output like:

```
100 comes first {"id":1,...}
```

### Configuration

| Field                              | Value          | Description                                              |
|------------------------------------|----------------|----------------------------------------------------------|
| `BatchConfiguration.MessageGroupLimit` | 1000       | Maximum messages collected before calling `BatchConsumeFn` |
| `MessageGroupDuration`             | 1s             | Maximum wait time before flushing an incomplete batch    |
| `RetryEnabled`                     | true           | Enables retry for batches that return an error           |
| `RetryConfiguration.MaxRetry`      | 3              | Maximum retry attempts per batch                         |

### Key Difference from Single Consumer

| Feature              | Single Consumer     | Batch Consumer                    |
|----------------------|---------------------|-----------------------------------|
| Handler signature    | `func(*Message) error` | `func([]*Message) error`       |
| Throughput           | Lower               | Higher (amortises per-call cost)  |
| Error granularity    | Per message         | Entire batch is retried on error  |

### Limitation

When `BatchConsumeFn` returns an error the entire batch is retried, not individual messages.
Ensure your handler is idempotent, or implement internal per-message error tracking and always
return `nil` from `BatchConsumeFn` after handling partial failures.
