package kafka

import (
	"errors"
	"sync"
	"testing"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	segmentio "github.com/segmentio/kafka-go"
)

func gatherLatency(t *testing.T, registry *prometheus.Registry, name string) []*dto.Metric {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.Metric
		}
	}
	return nil
}

func TestMessageQueueLatencyCollection(t *testing.T) {
	metric := &ConsumerMetric{}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewMetricCollector("", metric), NewMetricCollector("custom", metric))
	if got := gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds"); len(got) != 0 {
		t.Fatal("latency must not have observations before messages arrive")
	}

	now := time.Now()
	for _, age := range []time.Duration{time.Second, 30 * time.Second, 601 * time.Second} {
		metric.observeMessageQueueLatency(&Message{Topic: "orders", Time: now.Add(-age)}, "orders-group", now)
	}
	metric.observeMessageQueueLatency(&Message{Topic: "orders"}, "orders-group", now)
	metric.observeMessageQueueLatency(&Message{Topic: "orders", Time: now.Add(time.Second)}, "orders-group", now)
	metric.observeMessageQueueLatency(&Message{Topic: "payments", Time: now}, "payments-group", now)

	for _, prefix := range []string{"kafka_konsumer", "custom"} {
		metrics := gatherLatency(t, registry, prefix+"_message_queue_latency_seconds")
		if len(metrics) != 2 {
			t.Fatalf("expected two topic/group series, got %d", len(metrics))
		}
		for _, m := range metrics {
			labels := make(map[string]string)
			for _, label := range m.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["topic"] != "orders" {
				continue
			}
			if labels["consumer_group"] != "orders-group" || len(labels) != 2 {
				t.Fatalf("unexpected labels: %v", labels)
			}
			h := m.GetHistogram()
			if h.GetSampleCount() != 3 || h.GetSampleSum() != 632 {
				t.Fatalf("invalid timestamps must be excluded: %v", h)
			}
			if len(h.Bucket) != 14 {
				t.Fatalf("expected 14 finite buckets, got %d", len(h.Bucket))
			}
			for _, bucket := range h.Bucket {
				if bucket.GetUpperBound() == 600 && bucket.GetCumulativeCount() != 2 {
					t.Fatalf("600s bucket should exclude the delayed message: %v", bucket)
				}
			}
		}
	}

	// A newly created collector must retain observations made before registration.
	second := prometheus.NewPedanticRegistry()
	second.MustRegister(NewMetricCollector("", metric))
	if got := gatherLatency(t, second, "kafka_konsumer_message_queue_latency_seconds"); len(got) != 2 {
		t.Fatal("recreating collectors lost observations")
	}
}

func TestMessageQueueLatencyBeforeHandler(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			name := "single"
			if batch {
				name = "batch"
			}
			if fail {
				name += "_transactional_retry"
			}
			t.Run(name, func(t *testing.T) {
				b := &base{
					metric: &ConsumerMetric{}, consumerGroup: "group",
					transactionalRetry: fail, logger: NewZapLogger(LogLevelError),
				}
				registry := prometheus.NewPedanticRegistry()
				registry.MustRegister(b.GetMetricCollectors()...)
				messages := []*Message{{Topic: "orders", Time: time.Now().Add(-time.Minute)}}
				if batch {
					messages = append(messages, &Message{Topic: "orders", Time: messages[0].Time.Add(-time.Minute)})
				}
				calls := 0
				handler := func() error {
					calls++
					metrics := gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds")
					if len(metrics) != 1 || metrics[0].GetHistogram().GetSampleCount() != uint64(len(messages)) {
						t.Fatalf("each message must be observed before the first handler, without retry duplicates: %v", metrics)
					}
					if metrics[0].GetHistogram().GetSampleSum() < float64(len(messages))*60 {
						t.Fatal("message waiting time was not included")
					}
					if fail {
						return errors.New("handler failed")
					}
					return nil
				}
				if batch {
					c := batchConsumer{base: b, consumeFn: func([]*Message) error { return handler() }}
					c.process(messages)
				} else {
					c := consumer{base: b, consumeFn: func(*Message) error { return handler() }}
					c.process(messages[0])
				}
				wantCalls := 1
				if fail {
					wantCalls = 2
				}
				if calls != wantCalls {
					t.Fatalf("expected %d handler calls, got %d", wantCalls, calls)
				}
			})
		}
	}
}

func TestMessageQueueLatencyLongDelays(t *testing.T) {
	metric := &ConsumerMetric{}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewMetricCollector("", metric))
	now := time.Now()
	for _, seconds := range []int{600, 601, 1200, 1201, 1800, 1801, 3600, 3601} {
		metric.observeMessageQueueLatency(&Message{
			Topic: "orders", Time: now.Add(-time.Duration(seconds) * time.Second),
		}, "group", now)
	}
	metrics := gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds")
	if len(metrics) != 1 {
		t.Fatalf("expected one histogram, got %d", len(metrics))
	}
	histogram := metrics[0].GetHistogram()
	if histogram.GetSampleCount() != 8 || histogram.GetSampleSum() != 14404 {
		t.Fatalf("unexpected histogram totals: %v", histogram)
	}
	counts := make(map[float64]uint64)
	for _, bucket := range histogram.Bucket {
		counts[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
	}
	for bound, want := range map[float64]uint64{600: 1, 1200: 3, 1800: 5, 3600: 7} {
		if got, ok := counts[bound]; !ok || got != want {
			t.Errorf("bucket %g: expected %d observations, got %d (present=%t)", bound, want, got, ok)
		}
	}
}

func TestMessageQueueLatencyExcludesScheduledRetry(t *testing.T) {
	b := &base{metric: &ConsumerMetric{}}
	message := kcronsumer.Message{Topic: "retry", Time: time.Now().Add(-time.Hour)}
	single := consumer{base: b, consumeFn: func(*Message) error { return nil }}
	batch := batchConsumer{base: b, consumeFn: func([]*Message) error { return nil }}
	if err := single.runKonsumerFn(message); err != nil {
		t.Fatal(err)
	}
	if err := batch.runKonsumerFn(message); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(b.GetMetricCollectors()...)
	if got := gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds"); len(got) != 0 {
		t.Fatal("scheduled retries must not contribute latency observations")
	}
}

func TestMessageQueueLatencyConcurrentCollection(t *testing.T) {
	metric := &ConsumerMetric{}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewMetricCollector("", metric))
	now := time.Now()
	message := &Message{Topic: "orders", Time: now.Add(-time.Second)}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				metric.observeMessageQueueLatency(message, "group", now)
			}
		}()
	}
	for i := 0; i < 10; i++ {
		gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds")
	}
	wg.Wait()
	metrics := gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds")
	if len(metrics) != 1 || metrics[0].GetHistogram().GetSampleCount() != 800 {
		t.Fatalf("concurrent observations were lost: %v", metrics)
	}
}

func TestMessageQueueLatencyAfterPreBatch(t *testing.T) {
	for _, test := range []struct {
		name    string
		indices []int
		mutate  bool
	}{
		{name: "replacement drops Time", indices: []int{0, 1, 2, 3}},
		{name: "hook overwrites Time in place", indices: []int{0, 1, 2, 3}, mutate: true},
		{name: "filter by partition", indices: []int{1}},
		{name: "filter by offset", indices: []int{2}},
		{name: "filter by topic", indices: []int{3}},
		{name: "reorder replacements", indices: []int{3, 2, 1, 0}},
		{name: "filter and reorder", indices: []int{3, 1}},
		{name: "filter all"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := &base{
				metric: &ConsumerMetric{}, consumerGroup: "group", r: &mockReader{}, concurrency: 2,
				batchConsumingStream: make(chan []*Message, 4), messageProcessedStream: make(chan struct{}),
			}
			registry := prometheus.NewPedanticRegistry()
			registry.MustRegister(b.GetMetricCollectors()...)
			now := time.Now()
			messages := []*Message{
				{Topic: "orders", Partition: 0, Offset: 1, Time: now.Add(-time.Minute)},
				{Topic: "orders", Partition: 1, Offset: 1, Time: now.Add(-2 * time.Minute)},
				{Topic: "orders", Partition: 0, Offset: 2, Time: now.Add(-3 * time.Minute)},
				{Topic: "payments", Partition: 0, Offset: 1, Time: now.Add(-4 * time.Minute)},
			}
			wantCount := make(map[string]uint64)
			wantSum := make(map[string]float64)
			for _, index := range test.indices {
				message := messages[index]
				wantCount[message.Topic]++
				wantSum[message.Topic] += now.Sub(message.Time).Seconds()
			}
			c := batchConsumer{
				base: b, messageGroupLimit: 1,
				preBatchFn: func(messages []*Message) []*Message {
					result := make([]*Message, 0, len(test.indices))
					for _, index := range test.indices {
						message := messages[index]
						if test.mutate {
							message.Time = time.Now()
						} else {
							message = &Message{Topic: message.Topic, Partition: message.Partition, Offset: message.Offset}
						}
						result = append(result, message)
					}
					return result
				},
				consumeFn: func(messages []*Message) error {
					for _, message := range messages {
						if !test.mutate && !message.Time.IsZero() {
							t.Error("metric collection must not change the hook output")
						}
					}
					return nil
				},
			}
			commits := []segmentio.Message{}
			bytes := 0
			b.wg.Add(b.concurrency)
			c.setupConcurrentWorkers()
			c.consume(&messages, &commits, &bytes)
			close(b.batchConsumingStream)
			b.wg.Wait()
			elapsed := time.Since(now).Seconds()

			metrics := gatherLatency(t, registry, "kafka_konsumer_message_queue_latency_seconds")
			if len(metrics) != len(wantCount) {
				t.Fatalf("expected %d topic histograms, got %v", len(wantCount), metrics)
			}
			for _, metric := range metrics {
				var topic string
				for _, label := range metric.Label {
					if label.GetName() == "topic" {
						topic = label.GetValue()
					}
				}
				histogram := metric.GetHistogram()
				if histogram.GetSampleCount() != wantCount[topic] {
					t.Errorf("topic %s: expected %d observations, got %d", topic, wantCount[topic], histogram.GetSampleCount())
				}
				lower := wantSum[topic]
				upper := lower + elapsed*float64(wantCount[topic])
				if sum := histogram.GetSampleSum(); sum < lower || sum > upper {
					t.Errorf("topic %s: expected original timestamp latency in [%f, %f], got %f", topic, lower, upper, sum)
				}
			}
		})
	}
}
