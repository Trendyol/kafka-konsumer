### Prometheus Metrics

This example demonstrates how to enable the built-in Prometheus metrics endpoint in `kafka-konsumer`.
When `APIEnabled` is set to `true`, the library exposes an HTTP server with a `/metrics` endpoint
compatible with Prometheus scraping.

### Prerequisites

- Go 1.21+
- A running Kafka broker (see [docker-compose.yml](../docker-compose.yml))
- (Optional) A running Prometheus instance to scrape metrics

### How to run demo?

Start the Kafka broker:

```sh
docker-compose up -d
```

Run the consumer:

```sh
go run main.go
```

Once running, metrics are available at:

```
http://localhost:8090/metrics
```

You can verify with:

```sh
curl http://localhost:8090/metrics
```

### Key Configuration

| Field        | Value  | Description                                          |
|--------------|--------|------------------------------------------------------|
| `APIEnabled` | true   | Enables the built-in HTTP metrics server             |
| `LogLevel`   | Debug  | Verbose logging for development and troubleshooting  |

### Available Metrics

`kafka-konsumer` exposes consumer-level metrics including:

- Total messages consumed
- Total messages failed
- Retry queue depth
- Consumer lag (offset-based)

### Limitation

The default metrics port is `8090`. If you need to change it, refer to the `ConsumerConfig.APIPort`
field. The metrics endpoint does not require authentication; in production environments ensure network
policies restrict access appropriately.
