package main

import (
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
)

type messageProcessor struct{}

var _ kafka.Processor = (*messageProcessor)(nil)

func NewProcessor() kafka.Processor {
	return &messageProcessor{}
}

func (m messageProcessor) Process(message kafka.Message) {
	fmt.Println(message)
}
