package kafka

import (
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const Name = "kafka_konsumer"

type metricCollector struct {
	consumerMetric *ConsumerMetric

	totalUnprocessedMessagesCounter      *prometheus.Desc
	totalProcessedMessagesCounter        *prometheus.Desc
	totalUnprocessedBatchMessagesCounter *prometheus.Desc
	totalProcessedBatchMessagesCounter   *prometheus.Desc
}

func (s *metricCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *metricCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		s.totalProcessedMessagesCounter,
		prometheus.CounterValue,
		float64(s.consumerMetric.TotalProcessedMessagesCounter),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalUnprocessedMessagesCounter,
		prometheus.CounterValue,
		float64(s.consumerMetric.TotalUnprocessedMessagesCounter),
		[]string{}...,
	)
	ch <- prometheus.MustNewConstMetric(
		s.totalProcessedBatchMessagesCounter,
		prometheus.CounterValue,
		float64(s.consumerMetric.TotalProcessedBatchMessagesCounter),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalUnprocessedBatchMessagesCounter,
		prometheus.CounterValue,
		float64(s.consumerMetric.TotalUnprocessedBatchMessagesCounter),
		[]string{}...,
	)
}

func newMetricCollector(consumerMetric *ConsumerMetric) *metricCollector {
	return &metricCollector{
		consumerMetric: consumerMetric,

		totalProcessedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "processed_messages_total", "current"),
			"Total number of processed messages.",
			[]string{},
			nil,
		),
		totalUnprocessedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "unprocessed_messages_total", "current"),
			"Total number of unprocessed messages.",
			[]string{},
			nil,
		),
		totalProcessedBatchMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "processed_batch_messages_total", "current"),
			"Total number of processed batch messages.",
			[]string{},
			nil,
		),
		totalUnprocessedBatchMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "unprocessed_batch_messages_total", "current"),
			"Total number of unprocessed batch messages.",
			[]string{},
			nil,
		),
	}
}

func NewMetricMiddleware(cfg *ConsumerConfig,
	app *fiber.App,
	consumerMetric *ConsumerMetric,
	metricCollectors ...prometheus.Collector,
) (func(ctx *fiber.Ctx) error, error) {
	prometheus.DefaultRegisterer.MustRegister(newMetricCollector(consumerMetric))
	prometheus.DefaultRegisterer.MustRegister(metricCollectors...)

	fiberPrometheus := fiberprometheus.New(cfg.Reader.GroupID)
	fiberPrometheus.RegisterAt(app, *cfg.MetricConfiguration.Path)

	return fiberPrometheus.Middleware, nil
}
