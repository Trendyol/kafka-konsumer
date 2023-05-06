package instrumentation

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var TotalProcessedMessagesCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "kafka_kronsumer_processed_messages_total",
		Help: "Total number of processed messages.",
	},
)

var TotalProcessedRetryableMessagesCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "kafka_kronsumer_processed_retry_messages_total",
		Help: "Total number of processed retryable messages.",
	},
)
