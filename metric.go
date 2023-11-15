package kafka

type ConsumerMetric struct {
	TotalUnprocessedMessagesCounter int64
	TotalProcessedMessagesCounter   int64
}
