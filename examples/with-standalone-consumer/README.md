### Standalone Consumer

This example demonstrates the simplest way to consume messages from a Kafka topic using `kafka-konsumer`.
It sets up a single-partition consumer with no retry logic — ideal for getting started quickly.

### Prerequisites

- Go 1.21+
- A running Kafka broker (see [docker-compose.yml](../docker-compose.yml))

### How to run demo?

Start the Kafka broker using the shared docker-compose file at the examples root:

```sh
docker-compose up -d
```

Then run the consumer:

```sh
go run main.go
```

You should see messages printed to stdout as they are consumed from `standart-topic`.

To produce test messages into the topic you can use the Kafka console producer:

```sh
kafka-console-producer --broker-list localhost:29092 --topic standart-topic
```

### Configuration

| Field         | Value            | Description                          |
|---------------|------------------|--------------------------------------|
| `Brokers`     | localhost:29092  | Kafka broker address                 |
| `Topic`       | standart-topic   | Topic to consume from                |
| `GroupID`     | standart-cg      | Consumer group identifier            |
| `Concurrency` | 1                | Number of concurrent message workers |
| `RetryEnabled`| false            | Retry is disabled in this example    |

### Limitation

This example has no retry or dead-letter configuration. Failed messages will be skipped.
For retry support, see the [with-deadletter](../with-deadletter) example.
