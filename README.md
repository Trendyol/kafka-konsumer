# Kafka Konsumer

## Description

Kafka Konsumer provides an easy implementation of Kafka consumer with a built-in retry/exception manager ([kafka-cronsumer](https://github.com/Trendyol/kafka-cronsumer)).

## Guide

### Installation

### Examples

You can find a number of ready-to-run examples at [this directory](examples).

After running `docker-compose up` command, you can run any application you want.

#### Without Retry/Exception Manager
```go
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

```

#### With Retry/Exception Option Enabled

```go
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
```