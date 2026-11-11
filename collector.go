package kafka

import (
	"sync/atomic"

	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const Name = "kafka_konsumer"

type MetricCollector struct {
	consumerMetric *ConsumerMetric

	totalUnprocessedMessagesCounter      *prometheus.Desc
	totalProcessedMessagesCounter        *prometheus.Desc
	totalErrorCountDuringFetchingMessage *prometheus.Desc
	messageQueueLatency                  *prometheus.Desc
}

func NewMetricCollector(metricPrefix string, consumerMetric *ConsumerMetric) *MetricCollector {
	if metricPrefix == "" {
		metricPrefix = Name
	}

	return &MetricCollector{
		consumerMetric: consumerMetric,
		messageQueueLatency: prometheus.NewDesc(
			prometheus.BuildFQName(metricPrefix, "", "message_queue_latency_seconds"),
			messageQueueLatencyHelp,
			[]string{"topic", "consumer_group"},
			nil,
		),

		totalProcessedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(metricPrefix, "processed_messages_total", "current"),
			"Total number of processed messages.",
			emptyStringList,
			nil,
		),
		totalUnprocessedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(metricPrefix, "unprocessed_messages_total", "current"),
			"Total number of unprocessed messages.",
			emptyStringList,
			nil,
		),
		totalErrorCountDuringFetchingMessage: prometheus.NewDesc(
			prometheus.BuildFQName(metricPrefix, "error_count_during_fetching_message_total", "current"),
			"Total number of error during fetching message.",
			emptyStringList,
			nil,
		),
	}
}

func (s *MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.totalProcessedMessagesCounter
	ch <- s.totalUnprocessedMessagesCounter
	ch <- s.totalErrorCountDuringFetchingMessage
	ch <- s.messageQueueLatency
}

var emptyStringList []string

func (s *MetricCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		s.totalProcessedMessagesCounter,
		prometheus.CounterValue,
		float64(atomic.LoadInt64(&s.consumerMetric.totalProcessedMessagesCounter)),
		emptyStringList...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalUnprocessedMessagesCounter,
		prometheus.CounterValue,
		float64(atomic.LoadInt64(&s.consumerMetric.totalUnprocessedMessagesCounter)),
		emptyStringList...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalErrorCountDuringFetchingMessage,
		prometheus.CounterValue,
		float64(atomic.LoadInt64(&s.consumerMetric.totalErrorCountDuringFetchingMessage)),
		emptyStringList...,
	)

	for labels, histogram := range s.consumerMetric.latencySnapshot() {
		buckets := make(map[float64]uint64, len(messageQueueLatencyBuckets))
		for i, upperBound := range messageQueueLatencyBuckets {
			buckets[upperBound] = histogram.buckets[i]
		}
		ch <- prometheus.MustNewConstHistogram(
			s.messageQueueLatency,
			histogram.count,
			histogram.sum,
			buckets,
			labels.topic,
			labels.consumerGroup,
		)
	}
}

func NewMetricMiddleware(cfg *ConsumerConfig,
	app *fiber.App,
	consumerMetric *ConsumerMetric,
	metricCollectors ...prometheus.Collector,
) (func(ctx *fiber.Ctx) error, error) {
	prometheus.DefaultRegisterer.MustRegister(NewMetricCollector(cfg.MetricPrefix, consumerMetric))
	prometheus.DefaultRegisterer.MustRegister(metricCollectors...)

	fiberPrometheus := fiberprometheus.New(cfg.Reader.GroupID)
	fiberPrometheus.RegisterAt(app, *cfg.MetricConfiguration.Path)

	return fiberPrometheus.Middleware, nil
}
