package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
	segmentio "github.com/segmentio/kafka-go"
	"testing"
	"time"
)

func Test_Should_Produce_Successfully(t *testing.T) {
	// Given
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func Test_Should_Pause_And_Resume_Successfully(t *testing.T) {
	// Given
	t.Parallel()
	topic := "pause-topic"
	consumerGroup := "pause-topic-cg"
	brokerAddress := "localhost:9092"

	conn, cleanUp := createTopicAndWriteMessages(t, topic, nil)
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

	producer := &segmentio.Writer{
		Topic:                  topic,
		Addr:                   segmentio.TCP(brokerAddress),
		AllowAutoTopicCreation: true,
	}

	// When
	consumer.Pause()

	err := producer.WriteMessages(context.Background(), []segmentio.Message{{}, {}, {}}...)
	if err != nil {
		t.Fatalf("error producing step %s", err.Error())
	}

	// Then
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()

	select {
	case <-timeoutCtx.Done():
		o, _ := conn.ReadLastOffset()
		if o != 3 {
			t.Fatalf("offset %v must be equal to 3", o)
		}
	case <-messageCh:
		t.Fatal("Consumer is Pause Mode so it is not possible to consume message!")
	}

	// When
	consumer.Resume()

	// Then
	<-messageCh
	<-messageCh
	<-messageCh
}

func Test_Should_Batch_Consume_Messages_Successfully(t *testing.T) {
	// Given
	t.Parallel()
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
		MessageGroupDuration: time.Second,
		Reader:               kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit: 100,
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

func Test_Should_Batch_Retry_Only_Failed_Messages_When_Transactional_Retry_Is_Disabled(t *testing.T) {
	// Given
	t.Parallel()
	topic := "nontransactional-cronsumer-topic"
	consumerGroup := "nontransactional-cronsumer-cg"
	brokerAddress := "localhost:9092"

	retryTopic := "nontransactional-retry-topic"

	_, cleanUp := createTopicAndWriteMessages(t, topic, []segmentio.Message{
		{Topic: topic, Partition: 0, Offset: 1, Key: []byte("1"), Value: []byte(`foo1`)},
		{Topic: topic, Partition: 0, Offset: 2, Key: []byte("2"), Value: []byte(`foo2`)},
		{Topic: topic, Partition: 0, Offset: 3, Key: []byte("3"), Value: []byte(`foo3`)},
		{Topic: topic, Partition: 0, Offset: 4, Key: []byte("4"), Value: []byte(`foo4`)},
		{Topic: topic, Partition: 0, Offset: 5, Key: []byte("5"), Value: []byte(`foo5`)},
	})
	defer cleanUp()

	retryConn, cleanUpThisToo := createTopicAndWriteMessages(t, retryTopic, nil)
	defer cleanUpThisToo()

	consumerCfg := &kafka.ConsumerConfig{
		TransactionalRetry: kafka.NewBoolPtr(false),
		Reader:             kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		RetryEnabled:       true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:       []string{brokerAddress},
			Topic:         retryTopic,
			StartTimeCron: "*/5 * * * *",
			WorkDuration:  4 * time.Minute,
			MaxRetry:      3,
		},
		MessageGroupDuration: 20 * time.Second,
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit: 5,
			BatchConsumeFn: func(messages []*kafka.Message) error {
				messages[1].IsFailed = true
				return errors.New("err")
			},
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

func Test_Should_Integrate_With_Kafka_Cronsumer_Successfully(t *testing.T) {
	// Given
	t.Parallel()
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

func Test_Should_Propagate_Custom_Headers_With_Kafka_Cronsumer_Successfully(t *testing.T) {
	// Given
	t.Parallel()
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
		t.Fatal("error reading message")
	}
	if len(msg.Headers) != 2 {
		t.Fatal("msg header must be length of 2")
	}
	if msg.Headers[0].Key != "custom_exception_header" {
		t.Fatal("key must be custom_exception_header")
	}
	if !bytes.Equal(msg.Headers[0].Value, []byte("custom_exception_value")) {
		t.Fatal("value must be custom_exception_value")
	}
	if msg.Headers[1].Key != "x-error-message" {
		t.Fatal("key must be x-error-message")
	}
	if !bytes.Equal(msg.Headers[1].Value, []byte("err occurred")) {
		t.Fatal("err occurred")
	}
}

func Test_Should_Batch_Consume_With_PreBatch_Enabled(t *testing.T) {
	// Given
	t.Parallel()
	topic := "batch-topic-prebatch-enabled"
	consumerGroup := "batch-topic-prebatch-cg"
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
		MessageGroupDuration: time.Second,
		Reader:               kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit: 100,
			PreBatchFn: func(messages []*kafka.Message) []*kafka.Message {
				// assume that, there is couple of logic here
				return messages[:3]
			},
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

	if actual != 3 {
		t.Fatalf("Message length does not equal %d", actual)
	}

	o, _ := conn.ReadLastOffset()
	if o != 5 {
		t.Fatalf("offset %v must be equal to 5", o)
	}
}

func Test_Should_Skip_Message_When_Header_Filter_Given(t *testing.T) {
	// Given
	t.Parallel()
	topic := "header-filter-topic"
	consumerGroup := "header-filter-cg"
	brokerAddress := "localhost:9092"

	incomingMessage := []segmentio.Message{
		{
			Topic: topic,
			Headers: []segmentio.Header{
				{Key: "SkipMessage", Value: []byte("any")},
			},
			Key:   []byte("1"),
			Value: []byte(`foo`),
		},
	}

	_, cleanUp := createTopicAndWriteMessages(t, topic, incomingMessage)
	defer cleanUp()

	consumeCh := make(chan struct{})
	skipMessageCh := make(chan struct{})

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: topic, GroupID: consumerGroup},
		SkipMessageByHeaderFn: func(header []kafka.Header) bool {
			defer func() {
				skipMessageCh <- struct{}{}
			}()
			for _, h := range header {
				if h.Key == "SkipMessage" {
					return true
				}
			}
			return false
		},
		ConsumeFn: func(message *kafka.Message) error {
			consumeCh <- struct{}{}
			return nil
		},
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then
	<-skipMessageCh

	select {
	case <-consumeCh:
		t.Fatal("Message must be skipped! consumeCh mustn't receive any value")
	case <-time.After(1 * time.Second):
	}
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
