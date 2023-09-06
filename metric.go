package kafka

type ConsumerMetric struct {
	TotalUnprocessedMessagesCounter int64
	TotalProcessedMessagesCounter   int64
	// Deprecated
	TotalUnprocessedBatchMessagesCounter int64
	// Deprecated
	TotalProcessedBatchMessagesCounter int64
}
