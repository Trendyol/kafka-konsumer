package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	segmentio "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_Should_Produce_Successfully(t *testing.T) {
	// Given
	topic := "produce-topic"
	brokerAddress := "localhost:9092"

	producer, _ := kafka.NewProducer(kafka.ProducerConfig{
		Writer: kafka.WriterConfig{AllowAutoTopicCreation: true, Topic: topic, Brokers: []string{brokerAddress}}})

	// When
	err := producer.Produce(context.Background(), kafka.Message{
		Key:   []byte("1"),
		Value: []byte(`foo`),
	})

	// Then
	assert.Nil(t, err)
}

func Test_Should_Batch_Produce_Successfully(t *testing.T) {
	// Given
	topic := "batch-produce-topic"
	brokerAddress := "localhost:9092"

	producer, _ := kafka.NewProducer(kafka.ProducerConfig{
		Writer: kafka.WriterConfig{AllowAutoTopicCreation: true, Topic: topic, Brokers: []string{brokerAddress}}})

	// When
	msgs := []kafka.Message{
		{
			Key:   []byte("1"),
			Value: []byte(`foo`),
		},
		{
			Key:   []byte("2"),
			Value: []byte(`bar`),
		},
	}

	// When
	err := producer.ProduceBatch(context.Background(), msgs)

	// Then
	assert.Nil(t, err)
}

func Test_Should_Consume_Message_Successfully(t *testing.T) {
	// Given
	topic := "topic"
	consumerGroup := "topic-cg"
	brokerAddress := "localhost:9092"

	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	messageCh := make(chan kafka.Message)

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		ConsumeFn: func(message kafka.Message) error {
			messageCh <- message
			return nil
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// When
	produceMessages(t, conn, segmentio.Message{
		Key:   []byte("1"),
		Value: []byte(`foo`),
	})

	// Then
	actual := <-messageCh
	if string(actual.Value) != "foo" {
		t.Fatalf("Value does not equal %s", actual.Value)
	}
	if string(actual.Key) != "1" {
		t.Fatalf("Key does not equal %s", actual.Key)
	}
}

func Test_Should_Batch_Consume_Messages_Successfully(t *testing.T) {
	// Given
	topic := "batch-topic"
	consumerGroup := "batch-topic-cg"
	brokerAddress := "localhost:9092"

	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	messagesLen := make(chan int)

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit:    100,
			MessageGroupDuration: time.Second,
			BatchConsumeFn: func(messages []kafka.Message) error {
				messagesLen <- len(messages)
				return nil
			},
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// When
	produceMessages(t, conn,
		segmentio.Message{Key: []byte("1"), Value: []byte(`foo1`)},
		segmentio.Message{Key: []byte("2"), Value: []byte(`foo2`)},
		segmentio.Message{Key: []byte("3"), Value: []byte(`foo3`)},
		segmentio.Message{Key: []byte("4"), Value: []byte(`foo4`)},
		segmentio.Message{Key: []byte("5"), Value: []byte(`foo5`)},
	)

	// Then
	actual := <-messagesLen

	if actual != 5 {
		t.Fatalf("Message length does not equal %d", actual)
	}
}

func Test_Should_Integrate_With_Kafka_Cronsumer_Successfully(t *testing.T) {
	// Given
	topic := "cronsumer-topic"
	consumerGroup := "cronsumer-cg"
	brokerAddress := "localhost:9092"

	retryTopic := "retry-topic"

	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	retryConn, cleanUpThisToo := createTopic(t, retryTopic)
	defer cleanUpThisToo()

	consumerCfg := &kafka.ConsumerConfig{
		Reader:       kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		RetryEnabled: true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:       []string{brokerAddress},
			Topic:         retryTopic,
			StartTimeCron: "*/1 * * * *",
			WorkDuration:  50 * time.Second,
			MaxRetry:      3,
		},
		ConsumeFn: func(message kafka.Message) error {
			return errors.New("err occurred")
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// When
	produceMessages(t, conn, segmentio.Message{Key: []byte("1"), Value: []byte(`foo`)})

	// Then
	var expectedOffset int64 = 1
	conditionFunc := func() bool {
		lastOffset, _ := retryConn.ReadLastOffset()
		return lastOffset == expectedOffset
	}

	assertEventually(t, conditionFunc, 45*time.Second, time.Second)
}

func createTopic(t *testing.T, topicName string) (*segmentio.Conn, func()) {
	t.Helper()

	conn, err := segmentio.DialLeader(context.Background(), "tcp", "localhost:9092", topicName, 0)
	if err != nil {
		t.Fatalf("error while creating topic %s", err)
	}

	cleanUp := func() {
		if err := conn.DeleteTopics(topicName); err != nil {
			fmt.Println("err deleting topic", err.Error())
		}
	}

	return conn, cleanUp
}

func produceMessages(t *testing.T, conn *segmentio.Conn, msgs ...segmentio.Message) {
	t.Helper()

	if _, err := conn.WriteMessages(msgs...); err != nil {
		t.Fatalf("Produce err %s", err.Error())
	}
}

func assertEventually(t *testing.T, condition func() bool, waitFor time.Duration, tick time.Duration) bool {
	t.Helper()

	ch := make(chan bool, 1)

	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for tick := ticker.C; ; {
		select {
		case <-timer.C:
			t.Errorf("Condition never satisfied")
			return false
		case <-tick:
			tick = nil
			go func() { ch <- condition() }()
		case v := <-ch:
			if v {
				return true
			}
			tick = ticker.C
		}
	}
}
