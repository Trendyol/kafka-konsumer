package kafka

type ConsumerMetric struct {
	TotalUnprocessedMessagesCounter      int64
	TotalProcessedMessagesCounter        int64
	TotalUnprocessedBatchMessagesCounter int64
	TotalProcessedBatchMessagesCounter   int64
}
