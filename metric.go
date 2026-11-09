package kafka

import (
	"sync"
	"sync/atomic"
	"time"
)

const messageQueueLatencyHelp = "Time from the Kafka message timestamp to the start of its initial consumer handler, in seconds."

var messageQueueLatencyBuckets = [...]float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600}

type latencyLabels struct {
	topic         string
	consumerGroup string
}

type latencyHistogram struct {
	count   uint64
	sum     float64
	buckets [len(messageQueueLatencyBuckets)]uint64
}

type ConsumerMetric struct {
	totalUnprocessedMessagesCounter      int64
	totalProcessedMessagesCounter        int64
	totalErrorCountDuringFetchingMessage int64
	latencyMu                            sync.RWMutex
	messageQueueLatency                  map[latencyLabels]latencyHistogram
}

// Copy histogram values under the lock so a scrape sees consistent counts and
// sums without holding up observations while sending metrics to Prometheus.
func (cm *ConsumerMetric) latencySnapshot() map[latencyLabels]latencyHistogram {
	cm.latencyMu.RLock()
	defer cm.latencyMu.RUnlock()

	snapshot := make(map[latencyLabels]latencyHistogram, len(cm.messageQueueLatency))
	for labels, histogram := range cm.messageQueueLatency {
		snapshot[labels] = histogram
	}
	return snapshot
}

func (cm *ConsumerMetric) observeMessageQueueLatency(message *Message, consumerGroup string, handlerStart time.Time) {
	if message.Time.IsZero() || message.Time.After(handlerStart) {
		return
	}

	latency := handlerStart.Sub(message.Time).Seconds()
	labels := latencyLabels{topic: message.Topic, consumerGroup: consumerGroup}
	cm.latencyMu.Lock()
	defer cm.latencyMu.Unlock()

	if cm.messageQueueLatency == nil {
		cm.messageQueueLatency = make(map[latencyLabels]latencyHistogram)
	}
	histogram := cm.messageQueueLatency[labels]
	histogram.count++
	histogram.sum += latency
	for i, upperBound := range messageQueueLatencyBuckets {
		if latency <= upperBound {
			histogram.buckets[i]++
		}
	}
	cm.messageQueueLatency[labels] = histogram
}

func (cm *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter(delta int64) {
	atomic.AddInt64(&cm.totalUnprocessedMessagesCounter, delta)
}

func (cm *ConsumerMetric) IncrementTotalProcessedMessagesCounter(delta int64) {
	atomic.AddInt64(&cm.totalProcessedMessagesCounter, delta)
}

func (cm *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage(delta int64) {
	atomic.AddInt64(&cm.totalErrorCountDuringFetchingMessage, delta)
}
