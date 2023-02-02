package listener

import (
	"context"
	segmentio "github.com/segmentio/kafka-go"
)

type MessageProcessor interface {
	Process(message segmentio.Message)
}

type KafkaConsumer interface {
	ReadMessage(ctx context.Context) (segmentio.Message, error)
	Stop()
}

type KafkaListener interface {
	Listen(processor MessageProcessor, concurrency int)
}

type kafkaListener struct {
	messageChannel chan segmentio.Message
	kafkaConsumer  KafkaConsumer
}

func NewKafkaListener(kafkaConsumer KafkaConsumer) KafkaListener {
	return &kafkaListener{
		messageChannel: make(chan segmentio.Message),
		kafkaConsumer:  kafkaConsumer,
	}
}

func (k *kafkaListener) Listen(processor MessageProcessor, concurrency int) {
	go k.listenMessage()

	for i := 0; i < concurrency; i++ {
		go k.processMessage(processor)
	}
}

func (k *kafkaListener) listenMessage() {
	for {
		message, err := k.kafkaConsumer.ReadMessage(context.Background())
		if err == nil {
			k.messageChannel <- message
		}
	}
}

func (k *kafkaListener) processMessage(processor MessageProcessor) {
	for record := range k.messageChannel {
		processor.Process(record)
	}
}
