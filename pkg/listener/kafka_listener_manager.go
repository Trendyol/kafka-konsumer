package listener

type ActiveListenerManager interface {
	RegisterAndStart(consumer KafkaConsumer, processor MessageProcessor, concurrency int)
	Stop()
}

func NewManager() ActiveListenerManager {
	return &activeListenerManager{
		activeConsumers: make([]KafkaConsumer, 0),
	}
}

type activeListenerManager struct {
	activeConsumers []KafkaConsumer
}

func (m *activeListenerManager) RegisterAndStart(consumer KafkaConsumer, processor MessageProcessor, concurrency int) {
	listenerClient := NewKafkaListener(consumer)
	listenerClient.Listen(processor, concurrency)
	m.activeConsumers = append(m.activeConsumers, consumer)
}

func (m *activeListenerManager) Stop() {
	for i := range m.activeConsumers {
		m.activeConsumers[i].Stop()
	}
}
