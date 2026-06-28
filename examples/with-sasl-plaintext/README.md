### SASL PLAINTEXT Authentication

This example demonstrates how to connect to a Kafka broker that requires SASL PLAINTEXT
authentication. This is a common setup in managed Kafka environments and internal clusters
that enforce access control via username and password.

### Prerequisites

- Go 1.21+
- A Kafka broker configured with SASL PLAINTEXT (see [docker-compose.yml](docker-compose.yml) in this directory)

### How to run demo?

Start the Kafka broker with SASL enabled using the local docker-compose file:

```sh
docker-compose up -d
```

> **Note:** This example has its own `docker-compose.yml` and `configs/` directory, separate from
> the shared one at the examples root, because it requires a broker with SASL configured.

Run the consumer:

```sh
go run main.go
```

### SASL Configuration

```go
SASL: &kafka.SASLConfig{
    Type:     kafka.MechanismPlain,
    Username: "client",
    Password: "client-secret",
},
```

| Field      | Description                                      |
|------------|--------------------------------------------------|
| `Type`     | Authentication mechanism — `MechanismPlain` here |
| `Username` | SASL username configured on the broker           |
| `Password` | SASL password configured on the broker           |

### Supported Mechanisms

`kafka-konsumer` supports the following SASL mechanisms:

- `MechanismPlain` — SASL/PLAIN (username + password, plaintext)
- `MechanismScramSha256` — SASL/SCRAM-SHA-256
- `MechanismScramSha512` — SASL/SCRAM-SHA-512

### Limitation

SASL PLAINTEXT transmits credentials without transport-level encryption. For production deployments
combine SASL with TLS to protect credentials in transit. See the `TLS` field in `ConsumerConfig`
for TLS setup.
