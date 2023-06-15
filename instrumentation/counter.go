package instrumentation

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var TotalProcessedMessagesCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "kafka_konsumer_processed_messages_total",
		Help: "Total number of processed messages.",
	},
)

var TotalUnprocessedMessagesCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "kafka_konsumer_unprocessed_messages_total",
		Help: "Total number of unprocessed messages.",
	},
)

var TotalProcessedBatchMessagesCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "kafka_konsumer_processed_batch_messages_total",
		Help: "Total number of processed batch messages.",
	},
)

var TotalUnprocessedBatchMessagesCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "kafka_konsumer_unprocessed_batch_messages_total",
		Help: "Total number of unprocessed batch messages.",
	},
)
