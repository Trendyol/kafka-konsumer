### Dead Letter Topic

This example demonstrates how to configure retry logic and a dead-letter topic using `kafka-konsumer`.
When message processing fails, the library automatically retries via a retry topic, and after exhausting
all retries it routes the message to a dead-letter topic for manual inspection.

### Prerequisites

- Go 1.21+
- A running Kafka broker (see [docker-compose.yml](../docker-compose.yml))
- Three Kafka topics:
  - `standart-topic` — main consumption topic
  - `retry-topic` — intermediate retry topic
  - `error-topic` — dead-letter topic

### How to run demo?

Start the Kafka broker:

```sh
docker-compose up -d
```

Run the consumer:

```sh
go run main.go
```

The `consumeFn` in this example always returns an error, so every message will be retried once and
then forwarded to `error-topic`. You can verify this by consuming from the dead-letter topic:

```sh
kafka-console-consumer --bootstrap-server localhost:29092 --topic error-topic --from-beginning
```

### Configuration

| Field                          | Value          | Description                                        |
|--------------------------------|----------------|----------------------------------------------------|
| `RetryEnabled`                 | true           | Enables automatic retry on processing failure      |
| `RetryConfiguration.MaxRetry` | 1              | Maximum number of retry attempts before dead-letter|
| `RetryConfiguration.Topic`    | retry-topic    | Topic where failed messages are temporarily queued |
| `RetryConfiguration.DeadLetterTopic` | error-topic | Final destination for unrecoverable messages  |
| `StartTimeCron`                | `*/1 * * * *` | Retry worker start schedule (every minute)         |
| `WorkDuration`                 | 50s            | Duration the retry worker runs per cron tick       |

### Limitation

The retry cron schedule runs on a one-minute interval, so there may be a short delay before retried
messages are reprocessed. For immediate retry behaviour consider using a shorter `WorkDuration` or
a more frequent cron expression.
