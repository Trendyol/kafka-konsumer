### Send Direct To Dead Letter

This example demonstrates how to send messages directly to a dead-letter topic by marking them in your consume function. It covers both single-message and batch consumption.

#### Prerequisites
- Docker and Docker Compose
- Go 1.20+

#### Start the infrastructure
From the `examples` directory run:

```sh
docker compose up -d
```

This brings up Kafka at `localhost:29092` and creates topics: `standart-topic`, `batch-topic`, `retry-topic`, `error-topic`.

#### Run the single-message example
From this folder:

```sh
go run main.go
```

What it does:
- Produces one message to `standart-topic`
- Starts a consumer with `DeadLetterTopic: "error-topic"`
- Inside `consumeFn`, sets `message.SendDirectToDeadLetter = true` and returns an error
- The consumer writes the message to the dead-letter topic with header `x-error-message`

You should see output like:
- "Single Consumer started...!"
- The message being processed and then sent to the dead-letter topic

#### Run the batch example
From this folder:

```sh
go run main.go batch
```

What it does:
- Produces several messages to `batch-topic`
- Starts a batch consumer with retry enabled and `RetryConfiguration.DeadLetterTopic: "error-topic"`
- In `batchConsumeFn`, marks specific messages with `SendDirectToDeadLetter = true` and optionally sets `ErrDescription`
- Returns an error to trigger the direct dead-letter flow for marked messages first; remaining messages follow normal retry/processing logic

#### How direct dead-lettering works
- Set `message.SendDirectToDeadLetter = true` in `ConsumeFn` or `BatchConsumeFn`
- Ensure your config specifies a dead-letter topic:
  - Prefer `ConsumerConfig.DeadLetterTopic`
  - Or set `RetryConfiguration.DeadLetterTopic` (used as fallback)
- When your consume function returns an error, marked messages are produced to the dead-letter topic. The header `x-error-message` is set to `message.ErrDescription` if provided; otherwise to the returned error message.

#### Inspect topics
- Kafka UI is available at http://localhost:8080
- Check `error-topic` to see direct dead-lettered messages. 