package kafka

import (
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const Name = "kafka_konsumer"

type MetricCollector struct {
	consumerMetric *ConsumerMetric

	totalUnprocessedMessagesCounter *prometheus.Desc
	totalProcessedMessagesCounter   *prometheus.Desc
}

func (s *MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

var emptyStringList []string

func (s *MetricCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		s.totalProcessedMessagesCounter,
		prometheus.CounterValue,
		float64(s.consumerMetric.TotalProcessedMessagesCounter),
		emptyStringList...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalUnprocessedMessagesCounter,
		prometheus.CounterValue,
		float64(s.consumerMetric.TotalUnprocessedMessagesCounter),
		emptyStringList...,
	)
}

func NewMetricCollector(consumerMetric *ConsumerMetric, prefix string) *MetricCollector {
	if prefix == "" {
		prefix = Name
	}

	return &MetricCollector{
		consumerMetric: consumerMetric,

		totalProcessedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(prefix, "processed_messages_total", "current"),
			"Total number of processed messages.",
			emptyStringList,
			nil,
		),
		totalUnprocessedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(prefix, "unprocessed_messages_total", "current"),
			"Total number of unprocessed messages.",
			emptyStringList,
			nil,
		),
	}
}

func NewMetricMiddleware(cfg *ConsumerConfig,
	app *fiber.App,
	consumerMetric *ConsumerMetric,
	metricCollectors ...prometheus.Collector,
) (func(ctx *fiber.Ctx) error, error) {
	prometheus.DefaultRegisterer.MustRegister(NewMetricCollector(consumerMetric, cfg.MetricPrefix))
	prometheus.DefaultRegisterer.MustRegister(metricCollectors...)

	fiberPrometheus := fiberprometheus.New(cfg.Reader.GroupID)
	fiberPrometheus.RegisterAt(app, *cfg.MetricConfiguration.Path)

	return fiberPrometheus.Middleware, nil
}
