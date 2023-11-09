package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer"
	segmentio "github.com/segmentio/kafka-go"
	"testing"
	"time"
)

func Test_Should_Produce_Successfully(t *testing.T) {
	// Given
	topic := "produce-topic"
	brokerAddress := "localhost:9092"

	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig{AllowAutoTopicCreation: true, Topic: topic, Brokers: []string{brokerAddress}},
		Transport: &kafka.TransportConfig{
			MetadataTopics: []string{
				topic,
			},
		},
	})

	// When
	err := producer.Produce(context.Background(), kafka.Message{
		Key:   []byte("1"),
		Value: []byte(`foo`),
	})

	// Then
	if err != nil {
		t.Fatalf("Error while producing err %s", err.Error())
	}
}

func Test_Should_Batch_Produce_Successfully(t *testing.T) {
	// Given
	topic := "batch-produce-topic"
	brokerAddress := "localhost:9092"

	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
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
	if err != nil {
		t.Fatalf("Error while producing err %s", err.Error())
	}
}

func Test_Should_Consume_Message_Successfully(t *testing.T) {
	// Given
	topic := "topic"
	consumerGroup := "topic-cg"
	brokerAddress := "localhost:9092"

	conn, cleanUp := createTopicAndWriteMessages(t, topic, []segmentio.Message{{Topic: topic, Key: []byte("1"), Value: []byte(`foo`)}})
	defer cleanUp()

	messageCh := make(chan *kafka.Message)

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		ConsumeFn: func(message *kafka.Message) error {
			messageCh <- message
			return nil
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then
	actual := <-messageCh
	if string(actual.Value) != "foo" {
		t.Fatalf("Value does not equal %s", actual.Value)
	}
	if string(actual.Key) != "1" {
		t.Fatalf("Key does not equal %s", actual.Key)
	}

	o, _ := conn.ReadLastOffset()
	if o != 1 {
		t.Fatalf("offset %v must be equal to 1", o)
	}
}

func Test_Should_Batch_Consume_Messages_Successfully(t *testing.T) {
	// Given
	topic := "batch-topic"
	consumerGroup := "batch-topic-cg"
	brokerAddress := "localhost:9092"

	messages := []segmentio.Message{
		{Topic: topic, Partition: 0, Offset: 1, Key: []byte("1"), Value: []byte(`foo1`)},
		{Topic: topic, Partition: 0, Offset: 2, Key: []byte("2"), Value: []byte(`foo2`)},
		{Topic: topic, Partition: 0, Offset: 3, Key: []byte("3"), Value: []byte(`foo3`)},
		{Topic: topic, Partition: 0, Offset: 4, Key: []byte("4"), Value: []byte(`foo4`)},
		{Topic: topic, Partition: 0, Offset: 5, Key: []byte("5"), Value: []byte(`foo5`)},
	}

	conn, cleanUp := createTopicAndWriteMessages(t, topic, messages)
	defer cleanUp()

	messagesLen := make(chan int)

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit:    100,
			MessageGroupDuration: time.Second,
			BatchConsumeFn: func(messages []*kafka.Message) error {
				messagesLen <- len(messages)
				return nil
			},
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then
	actual := <-messagesLen

	if actual != 5 {
		t.Fatalf("Message length does not equal %d", actual)
	}

	o, _ := conn.ReadLastOffset()
	if o != 5 {
		t.Fatalf("offset %v must be equal to 5", o)
	}
}

func Test_Should_Integrate_With_Kafka_Cronsumer_Successfully(t *testing.T) {
	// Given
	topic := "cronsumer-topic"
	consumerGroup := "cronsumer-cg"
	brokerAddress := "localhost:9092"

	retryTopic := "retry-topic"

	_, cleanUp := createTopicAndWriteMessages(t, topic, []segmentio.Message{{Topic: topic, Key: []byte("1"), Value: []byte(`foo`)}})
	defer cleanUp()

	retryConn, cleanUpThisToo := createTopicAndWriteMessages(t, retryTopic, nil)
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
			LogLevel:      "error",
		},
		ConsumeFn: func(message *kafka.Message) error {
			return errors.New("err occurred")
		},
		LogLevel: kafka.LogLevelError,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then
	var expectedOffset int64 = 1
	conditionFunc := func() bool {
		lastOffset, _ := retryConn.ReadLastOffset()
		return lastOffset == expectedOffset
	}

	assertEventually(t, conditionFunc, 45*time.Second, time.Second)
}

func Test_Should_Progate_Custom_Headers_With_Kafka_Cronsumer_Successfully(t *testing.T) {
	// Given
	topic := "cronsumer-header-topic"
	consumerGroup := "cronsumer-header-cg"
	brokerAddress := "localhost:9092"

	retryTopic := "exception-topic"

	_, cleanUp := createTopicAndWriteMessages(t, topic, []segmentio.Message{
		{Topic: topic, Key: []byte("1"), Value: []byte(`foo`)}},
	)
	defer cleanUp()

	retryConn, cleanUpThisToo := createTopicAndWriteMessages(t, retryTopic, nil)
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
			LogLevel:      "error",
		},
		ConsumeFn: func(message *kafka.Message) error {
			message.AddHeader(kafka.Header{Key: "custom_exception_header", Value: []byte("custom_exception_value")})

			return errors.New("err occurred")
		},
		LogLevel: kafka.LogLevelError,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then
	var expectedOffset int64 = 1
	conditionFunc := func() bool {
		lastOffset, _ := retryConn.ReadLastOffset()
		return lastOffset == expectedOffset
	}

	assertEventually(t, conditionFunc, 45*time.Second, time.Second)
	msg, err := retryConn.ReadMessage(10_000)
	if err != nil {
		t.Fatalf("error reading message")
	}
	if len(msg.Headers) != 1 {
		t.Fatalf("msg header must be length of 1")
	}
	if msg.Headers[0].Key != "custom_exception_header" {
		t.Fatalf("key must be custom_exception_header")
	}
	if !bytes.Equal(msg.Headers[0].Value, []byte("custom_exception_value")) {
		t.Fatalf("value must be custom_exception_value")
	}
	_ = msg
}

func createTopicAndWriteMessages(t *testing.T, topicName string, messages []segmentio.Message) (*segmentio.Conn, func()) {
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

	if messages != nil {
		producer := &segmentio.Writer{
			Addr:                   segmentio.TCP("localhost:9092"),
			AllowAutoTopicCreation: true,
		}

		err = producer.WriteMessages(context.Background(), messages...)
		if err != nil {
			t.Fatalf("err during write message %s", err.Error())
		}
	}

	return conn, cleanUp
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
