Before implementation, I researched [segmentio/kafka-go](https://github.com/segmentio/kafka-go)' issues for this 
functionality.  I came across [this issue](https://github.com/segmentio/kafka-go/issues/474). [Achille-roussel](https://github.com/achille-roussel)
who is the old maintainer of the kafka-go clearly said that 

```
To pause consuming from a partition, you can simply stop reading 
messages. Kafka does not have a concept of pausing or resuming in its protocol, the responsibility is given to clients 
to decide what to read and when.
```

It means, if we stop calling `FetchMessage`, the consumer pauses. If we invoke, the consumer resumes. Here, there is very
important behaviour exist. Consumer group state not affected at all in this situation. When we call `kafka.NewConsumer`,
segmentio/kafka-go library creates a goroutine under the hood, and it starts to send heartbeat with a specific interval
so even if we stop calling `FetchMessage`, consumer group still stable mode and not consumes new message at all.

```go
consumer, _ := kafka.NewConsumer(consumerCfg)
defer consumer.Stop()

consumer.Consume()
fmt.Println("Consumer started...!")

// consumer.Pause(), consumer.Resume()
```

If you need to implement Pause & Resume functionality on your own applications, you need to call `Consume`. Because this
method creates listeners goroutine under the hood. After that you can manage the lifecycle of the consumer by calling
`Pause` and `Resume` methods.

You can run the example to see `Is consumer consumes new message in Pause mode` or `consumer consumes new message in Resume mode`
by producing dummy messages on kowl ui.

