package main

import (
	"fmt"
	"github.com/Abdulsametileri/kafka-template/pkg/listener"
	segmentio "github.com/segmentio/kafka-go"
)

type messageProcessor struct{}

var _ listener.MessageProcessor = (*messageProcessor)(nil)

func NewProcessor() listener.MessageProcessor {
	return &messageProcessor{}
}

func (m messageProcessor) Process(message segmentio.Message) {
	fmt.Println(message)
}
