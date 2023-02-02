package main

import (
	"github.com/Abdulsametileri/kafka-template/pkg/config"
	"github.com/Abdulsametileri/kafka-template/pkg/kafka"
	"github.com/Abdulsametileri/kafka-template/pkg/listener"
	"log"
)

func main() {
	kafkaCfg := &config.Kafka{
		Servers: "localhost:29092",
	}
	kafkaConsumer := &config.Consumer{
		Concurrency:   10,
		Topics:        []string{"standart-topic"},
		ConsumerGroup: "standart-cg",
		Exception:     config.Exception{},
	}

	consumer, err := kafka.NewConsumer(kafkaCfg, kafkaConsumer)
	if err != nil {
		log.Fatal(err.Error())
	}

	processor := NewProcessor()

	activeListenerManager := listener.NewManager()
	activeListenerManager.RegisterAndStart(consumer, processor, kafkaConsumer.Concurrency)

	select {}
}
