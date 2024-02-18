package kafka

import (
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func Test_NewCollector(t *testing.T) {
	t.Run("When_Default_Prefix_Value_Used", func(t *testing.T) {
		cronsumerMetric := &ConsumerMetric{}
		expectedTotalProcessedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName(Name, "processed_messages_total", "current"),
			"Total number of processed messages.",
			emptyStringList,
			nil,
		)
		expectedTotalUnprocessedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName(Name, "unprocessed_messages_total", "current"),
			"Total number of unprocessed messages.",
			emptyStringList,
			nil,
		)

		collector := NewMetricCollector("", cronsumerMetric)

		if !reflect.DeepEqual(collector.totalProcessedMessagesCounter, expectedTotalProcessedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalProcessedMessagesCounter, expectedTotalProcessedMessagesCounter)
		}
		if !reflect.DeepEqual(collector.totalUnprocessedMessagesCounter, expectedTotalUnprocessedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalUnprocessedMessagesCounter, expectedTotalUnprocessedMessagesCounter)
		}
	})
	t.Run("When_Custom_Prefix_Value_Used", func(t *testing.T) {
		cronsumerMetric := &ConsumerMetric{}
		expectedTotalProcessedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName("custom_prefix", "processed_messages_total", "current"),
			"Total number of processed messages.",
			emptyStringList,
			nil,
		)
		expectedTotalUnprocessedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName("custom_prefix", "unprocessed_messages_total", "current"),
			"Total number of unprocessed messages.",
			emptyStringList,
			nil,
		)

		collector := NewMetricCollector("custom_prefix", cronsumerMetric)

		if !reflect.DeepEqual(collector.totalProcessedMessagesCounter, expectedTotalProcessedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalProcessedMessagesCounter, expectedTotalProcessedMessagesCounter)
		}
		if !reflect.DeepEqual(collector.totalUnprocessedMessagesCounter, expectedTotalUnprocessedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalUnprocessedMessagesCounter, expectedTotalUnprocessedMessagesCounter)
		}
	})
}
