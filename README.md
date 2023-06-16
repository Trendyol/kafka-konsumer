# Kafka Konsumer
<div style="text-align:center"><img src=".github/images/konsumer.png"/></div>

## Description

Kafka Konsumer provides an easy implementation of Kafka consumer with a built-in retry/exception
manager ([kafka-cronsumer](https://github.com/Trendyol/kafka-cronsumer)).

## Guide

### Installation

```sh
go get github.com/Trendyol/kafka-konsumer@latest
```

### Examples

You can find a number of ready-to-run examples at [this directory](examples).

After running `docker-compose up` command, you can run any application you want.

<details>
    <summary>Without Retry/Exception Manager</summary>

    func main() {
        consumerCfg := &kafka.ConsumerConfig{
            Reader: kafka.ReaderConfig{
                Brokers: []string{"localhost:29092"},
                Topic:   "standart-topic",
                GroupID: "standart-cg",
            },
            ConsumeFn:    consumeFn,
            RetryEnabled: false,
        }
    
        consumer, _ := kafka.NewConsumer(consumerCfg)
        defer consumer.Stop()
        
        consumer.Consume()
    }
    
    func consumeFn(message kafka.Message) error {
        fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))
        return nil
    }
</details>

<details>
    <summary>With Retry/Exception Option Enabled</summary>
    
    func main() {
        consumerCfg := &kafka.ConsumerConfig{
            Reader: kafka.ReaderConfig{
                Brokers: []string{"localhost:29092"},
                Topic:   "standart-topic",
                GroupID: "standart-cg",
            },
            RetryEnabled: true,
            RetryConfiguration: kafka.RetryConfiguration{
                Topic:         "retry-topic",
                StartTimeCron: "*/1 * * * *",
                WorkDuration:  50 * time.Second,
                MaxRetry:      3,
            },
            ConsumeFn: consumeFn,
        }
    
        consumer, _ := kafka.NewConsumer(consumerCfg)
        defer consumer.Stop()
        
        consumer.Consume()
    }
    
    func consumeFn(message kafka.Message) error {
        fmt.Printf("Message From %s with value %s", message.Topic, string(message.Value))
        return nil
    }
</details>

<details>
    <summary>With Batch Option</summary>

    func main() {
        consumerCfg := &kafka.ConsumerConfig{
            Reader: kafka.ReaderConfig{
                Brokers: []string{"localhost:29092"},
                Topic:   "standart-topic",
                GroupID: "standart-cg",
            },
            LogLevel:     kafka.LogLevelDebug,
            RetryEnabled: true,
            RetryConfiguration: kafka.RetryConfiguration{
                Brokers:       []string{"localhost:29092"},
                Topic:         "retry-topic",
                StartTimeCron: "*/1 * * * *",
                WorkDuration:  50 * time.Second,
                MaxRetry:      3,
            },
            BatchConfiguration: kafka.BatchConfiguration{
                MessageGroupLimit:    1000,
                MessageGroupDuration: time.Second,
                BatchConsumeFn:       batchConsumeFn,
            },
        }
    
        consumer, _ := kafka.NewConsumer(consumerCfg)
        defer consumer.Stop()
    
        consumer.Consume()
    }
    
    func batchConsumeFn(messages []kafka.Message) error {
        fmt.Printf("%d\n comes first %s", len(messages), messages[0].Value)
        return nil
    }
</details>


#### With Grafana & Prometheus

In this example, we are demonstrating how to create Grafana dashboard and how to define alerts in Prometheus. You can
see the example by going to the [with-grafana](examples/with-grafana) folder in the [examples](examples) folder
and running the infrastructure with `docker compose up` and then the application.

![grafana](.github/images/grafana.png)

#### With SASL-PLAINTEXT Authentication

Under the [examples](examples) - [with-sasl-plaintext](examples/with-sasl-plaintext) folder, you can find an example 
of a consumer integration with SASL/PLAIN mechanism. To try the example, you can run the command `docker compose up` 
under [the specified folder](examples/with-sasl-plaintext) and then start the application.

## Configurations

| config                                      | description                                                                                                                          | default     |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `reader`                                    | [Describes all segmentio kafka reader configurations](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.39#ReaderConfig)         |             |
| `consumeFn`                                 | Kafka consumer function, if retry enabled it, is also used to consume retriable messages                                             |             |
| `logLevel`                                  | Describes log level; valid options are `debug`, `info`, `warn`, and `error`                                                          | info        |
| `concurrency`                               | Number of goroutines used at listeners                                                                                               | 1           |
| `retryEnabled`                              | Retry/Exception consumer is working or not                                                                                           | false       |
| `rack`                                      | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go#RackAffinityGroupBalancer)                                                |             |
| `retryConfiguration.startTimeCron`          | Cron expression when retry consumer ([kafka-cronsumer](https://github.com/Trendyol/kafka-cronsumer#configurations)) starts to work at |             |
| `retryConfiguration.workDuration`           | Work duration exception consumer actively consuming messages                                                                         |             |
| `retryConfiguration.topic`                  | Retry/Exception topic names                                                                                                          |             |
| `retryConfiguration.brokers`                | Retry topic brokers urls                                                                                                             |             |
| `retryConfiguration.maxRetry`               | Maximum retry value for attempting to retry a message                                                                                | 3           |
| `retryConfiguration.tls.rootCAPath`         | [see doc](https://pkg.go.dev/crypto/tls#Config.RootCAs)                                                                              | ""          |
| `retryConfiguration.tls.intermediateCAPath` | Same with rootCA, if you want to specify two rootca you can use it with rootCAPath                                                   | ""          |
| `retryConfiguration.sasl.authType`          | `SCRAM` or `PLAIN`                                                                                                                   |             |
| `retryConfiguration.sasl.username`          | SCRAM OR PLAIN username                                                                                                              |             |
| `retryConfiguration.sasl.password`          | SCRAM OR PLAIN password                                                                                                              |             |
| `batchConfiguration.messageGroupLimit`      | Maximum number of messages in a batch                                                                                                |             |
| `batchConfiguration.messageGroupDuration`   | Maximum time to wait for a batch                                                                                                     |             |
| `tls.rootCAPath`                            | [see doc](https://pkg.go.dev/crypto/tls#Config.RootCAs)                                                                              | ""          |
| `tls.intermediateCAPath`                    | Same with rootCA, if you want to specify two rootca you can use it with rootCAPath                                                   | ""          |
| `sasl.authType`                             | `SCRAM` or `PLAIN`                                                                                                                   |             |
| `sasl.username`                             | SCRAM OR PLAIN username                                                                                                              |             |
| `sasl.password`                             | SCRAM OR PLAIN password                                                                                                              |             |
| `logger`                                    | If you want to custom logger                                                                                                         | info        |
| `apiEnabled`                                | Enabled metrics                                                                                                                      | false       |
| `apiConfiguration.port`                     | Set API port                                                                                                                         | 8090        |
| `apiConfiguration.healtCheckPath`           | Set Health check path                                                                                                                | healthcheck |
| `metricConfiguration.path`                  | Set metric endpoint path                                                                                                             | /metrics    |

## Monitoring

Kafka Konsumer offers an API that handles exposing several metrics.

### Exposed Metrics

| Metric Name                                     | Description                                 | Value Type |
|-------------------------------------------------|---------------------------------------------|------------|
| kafka_konsumer_processed_messages_total         | Total number of processed messages.         | Counter    |
| kafka_konsumer_processed_batch_messages_total   | Total number of processed batch messages.   | Counter    |
| kafka_konsumer_unprocessed_messages_total       | Total number of unprocessed messages.       | Counter    |
| kafka_konsumer_unprocessed_batch_messages_total | Total number of unprocessed batch messages. | Counter    |