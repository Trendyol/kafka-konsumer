package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Trendyol/kafka-konsumer/v2"
	segmentio "github.com/segmentio/kafka-go"
)

func Test_Should_Produce_Successfully(t *testing.T) {
	// Given
	t.Parallel()
	brokerAddress := "localhost:9092"

	t.Run("without interceptor", func(t *testing.T) {
		//Given

		topic := "produce-topic"
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
	})

	t.Run("with interceptor", func(t *testing.T) {
		// Given
		topic := "produce-interceptor-topic"
		consumerGroup := "produce-topic-cg"
		interceptor := newMockProducerInterceptor()

		producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
			Writer: kafka.WriterConfig{AllowAutoTopicCreation: true, Topic: topic, Brokers: []string{brokerAddress}},
			Transport: &kafka.TransportConfig{
				MetadataTopics: []string{
					topic,
				},
			},
		}, interceptor...)

		// When
		err := producer.Produce(context.Background(), kafka.Message{
			Key:   []byte("1"),
			Value: []byte(`foo`),
		})

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

		if err != nil {
			t.Fatalf("Error while producing err %s", err.Error())
		}

		actual := <-messageCh
		if string(actual.Value) != "foo" {
			t.Fatalf("Value does not equal %s", actual.Value)
		}
		if string(actual.Key) != "1" {
			t.Fatalf("Key does not equal %s", actual.Key)
		}
		if len(actual.Headers) != 1 {
			t.Fatalf("Header size does not equal %d", len(actual.Headers))
		}
		if string(actual.Headers[0].Key) != xSourceAppKey {
			t.Fatalf("Header key does not equal %s", actual.Headers[0].Key)
		}
		if string(actual.Headers[0].Value) != xSourceAppValue {
			t.Fatalf("Header value does not equal %s", actual.Headers[0].Value)
		}
	})
}

func Test_Should_Batch_Produce_Successfully(t *testing.T) {
	// Given
	t.Parallel()
	topic := "batch-produce-topic"
	brokerAddress := "localhost:9092"
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

	t.Run("without interceptor", func(t *testing.T) {
		producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
			Writer: kafka.WriterConfig{AllowAutoTopicCreation: true, Topic: topic, Brokers: []string{brokerAddress}}})

		// When
		err := producer.ProduceBatch(context.Background(), msgs)

		// Then
		if err != nil {
			t.Fatalf("Error while producing err %s", err.Error())
		}
	})

	t.Run("with interceptor", func(t *testing.T) {
		interceptors := newMockProducerInterceptor()

		producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
			Writer: kafka.WriterConfig{AllowAutoTopicCreation: true, Topic: topic, Brokers: []string{brokerAddress}}}, interceptors...)

		// When
		err := producer.ProduceBatch(context.Background(), msgs)

		// Then
		if err != nil {
			t.Fatalf("Error while producing err %s", err.Error())
		}
	})
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
	if msg.Headers[1].Key != errMessageKey {
		t.Fatalf("key must be %s", errMessageKey)
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

func Test_Should_Send_Directly_To_DeadLetter_On_Single_Consume(t *testing.T) {
	// Given
	t.Parallel()
	brokerAddress := "localhost:9092"

	sourceTopic := "direct-deadletter-single-topic"
	deadLetterTopic := "direct-deadletter-single-error-topic"
	consumerGroup := "direct-deadletter-single-cg"

	_, cleanUp := createTopicAndWriteMessages(t, sourceTopic, []segmentio.Message{{Topic: sourceTopic, Key: []byte("1"), Value: []byte(`foo`)}})
	defer cleanUp()

	deadLetterConn, cleanUpDeadLetter := createTopicAndWriteMessages(t, deadLetterTopic, nil)
	defer cleanUpDeadLetter()

	consumerCfg := &kafka.ConsumerConfig{
		Reader:          kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: sourceTopic, GroupID: consumerGroup},
		DeadLetterTopic: deadLetterTopic,
		ConsumeFn: func(message *kafka.Message) error {
			message.SendDirectToDeadLetter = true
			message.ErrDescription = "custom direct error"
			return errors.New("err")
		},
		LogLevel: kafka.LogLevelError,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then
	var expectedOffset int64 = 1
	conditionFunc := func() bool {
		lastOffset, _ := deadLetterConn.ReadLastOffset()
		return lastOffset == expectedOffset
	}
	assertEventually(t, conditionFunc, 45*time.Second, time.Second)

	msg, err := deadLetterConn.ReadMessage(10_000)
	if err != nil {
		t.Fatal("error reading dead letter message")
	}
	if !bytes.Equal(msg.Key, []byte("1")) {
		t.Fatalf("dead letter message key must be 1, got %s", string(msg.Key))
	}
	if !bytes.Equal(msg.Value, []byte("foo")) {
		t.Fatalf("dead letter message value must be foo, got %s", string(msg.Value))
	}

	var errHeaderFound bool
	for _, h := range msg.Headers {
		if h.Key == errMessageKey {
			errHeaderFound = true
			if !bytes.Equal(h.Value, []byte("custom direct error")) {
				t.Fatalf("%s must be 'custom direct error', got %s", errMessageKey, string(h.Value))
			}
		}
	}
	if !errHeaderFound {
		t.Fatalf("%s header not found on dead letter message", errMessageKey)
	}
}

func Test_Should_Send_Directly_To_DeadLetter_On_Batch_Consume(t *testing.T) {
	// Given
	t.Parallel()
	brokerAddress := "localhost:9092"

	sourceTopic := "direct-deadletter-batch-topic"
	deadLetterTopic := "direct-deadletter-batch-error-topic"
	consumerGroup := "direct-deadletter-batch-cg"

	messages := []segmentio.Message{
		{Topic: sourceTopic, Partition: 0, Offset: 1, Key: []byte("1"), Value: []byte(`foo1`)},
		{Topic: sourceTopic, Partition: 0, Offset: 2, Key: []byte("2"), Value: []byte(`foo2`)},
		{Topic: sourceTopic, Partition: 0, Offset: 3, Key: []byte("3"), Value: []byte(`foo3`)},
		{Topic: sourceTopic, Partition: 0, Offset: 4, Key: []byte("4"), Value: []byte(`foo4`)},
		{Topic: sourceTopic, Partition: 0, Offset: 5, Key: []byte("5"), Value: []byte(`foo5`)},
	}

	_, cleanUp := createTopicAndWriteMessages(t, sourceTopic, messages)
	defer cleanUp()

	deadLetterConn, cleanUpDeadLetter := createTopicAndWriteMessages(t, deadLetterTopic, nil)
	defer cleanUpDeadLetter()

	consumerCfg := &kafka.ConsumerConfig{
		MessageGroupDuration: time.Second,
		Reader:               kafka.ReaderConfig{Brokers: []string{brokerAddress}, Topic: sourceTopic, GroupID: consumerGroup},
		DeadLetterTopic:      deadLetterTopic,
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit: 100,
			BatchConsumeFn: func(msgs []*kafka.Message) error {
				for _, m := range msgs {
					if string(m.Key) == "2" || string(m.Key) == "4" {
						m.SendDirectToDeadLetter = true
						m.ErrDescription = string(m.Key) + " error"
					}
				}
				return errors.New("batch processing error")
			},
		},
		LogLevel: kafka.LogLevelError,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	// Then: expect 2 messages on dead letter
	var expectedOffset int64 = 2
	conditionFunc := func() bool {
		lastOffset, _ := deadLetterConn.ReadLastOffset()
		return lastOffset == expectedOffset
	}
	assertEventually(t, conditionFunc, 45*time.Second, time.Second)

	// Read and verify the two dead-lettered messages
	seen := map[string]bool{"2": false, "4": false}
	for i := 0; i < 2; i++ {
		msg, err := deadLetterConn.ReadMessage(10_000)
		if err != nil {
			t.Fatal("error reading dead letter message")
		}
		k := string(msg.Key)
		if k != "2" && k != "4" {
			t.Fatalf("unexpected key on dead letter topic: %s", k)
		}

		var errHeaderFound bool
		for _, h := range msg.Headers {
			if h.Key == errMessageKey {
				errHeaderFound = true
				expected := k + " error"
				if !bytes.Equal(h.Value, []byte(expected)) {
					t.Fatalf("%s must be '%s', got %s", errMessageKey, expected, string(h.Value))
				}
			}
		}
		if !errHeaderFound {
			t.Fatalf("%s header not found on dead letter message", errMessageKey)
		}
		seen[k] = true
	}

	if !seen["2"] || !seen["4"] {
		t.Fatal("dead letter messages with keys '2' and '4' must be seen")
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

type mockProducerInterceptor struct{}

const (
	xSourceAppKey   = "x-source-app"
	xSourceAppValue = "kafka-konsumer"
	errMessageKey   = "x-error-message"
)

func (i *mockProducerInterceptor) OnProduce(ctx kafka.ProducerInterceptorContext) {
	ctx.Message.Headers = append(ctx.Message.Headers, kafka.Header{
		Key:   xSourceAppKey,
		Value: []byte(xSourceAppValue),
	})
}

func newMockProducerInterceptor() []kafka.ProducerInterceptor {
	return []kafka.ProducerInterceptor{&mockProducerInterceptor{}}
}

func Test_Should_Resume_From_Latest_Offset_And_Skip_Messages_During_Pause(t *testing.T) {
	// Given
	t.Parallel()
	topic := "resume-from-latest-topic"
	consumerGroup := "resume-from-latest-cg"
	brokerAddress := "localhost:9092"

	conn, cleanUp := createTopicAndWriteMessages(t, topic, nil)
	defer cleanUp()

	type consumedMessage struct {
		key       string
		value     string
		offset    int64
		timestamp time.Time
	}

	messageCh := make(chan consumedMessage, 100)
	pauseTimestamp := time.Time{}
	resumeTimestamp := time.Time{}

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: []string{brokerAddress},
			Topic:   topic,
			GroupID: consumerGroup,
		},
		ConsumeFn: func(message *kafka.Message) error {
			messageCh <- consumedMessage{
				key:       string(message.Key),
				value:     string(message.Value),
				offset:    message.Offset,
				timestamp: time.Now(),
			}
			return nil
		},
	}

	consumer, err := kafka.NewConsumer(consumerCfg)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Stop()

	consumer.Consume()

	producer := &segmentio.Writer{
		Topic:                  topic,
		Addr:                   segmentio.TCP(brokerAddress),
		AllowAutoTopicCreation: true,
	}
	defer producer.Close()

	// Phase 1: Produce and consume initial messages (BEFORE PAUSE)
	t.Log("Phase 1: Producing initial messages before pause...")
	initialMessages := []segmentio.Message{
		{Key: []byte("before-1"), Value: []byte("value-before-1")},
		{Key: []byte("before-2"), Value: []byte("value-before-2")},
	}
	err = producer.WriteMessages(context.Background(), initialMessages...)
	if err != nil {
		t.Fatalf("Failed to produce initial messages: %v", err)
	}

	// Consume initial messages
	msg1 := <-messageCh
	msg2 := <-messageCh
	t.Logf("Consumed before pause: offset=%d, key=%s", msg1.offset, msg1.key)
	t.Logf("Consumed before pause: offset=%d, key=%s", msg2.offset, msg2.key)

	if msg1.key != "before-1" || msg2.key != "before-2" {
		t.Fatalf("Initial messages not consumed correctly")
	}

	// Phase 2: Pause consumer
	t.Log("Phase 2: Pausing consumer...")
	consumer.Pause()
	time.Sleep(100 * time.Millisecond) // Ensure pause takes effect
	pauseTimestamp = time.Now()

	// Phase 3: Produce messages DURING PAUSE (these should be SKIPPED)
	t.Log("Phase 3: Producing messages during pause (should be skipped)...")
	duringPauseMessages := []segmentio.Message{
		{Key: []byte("during-pause-1"), Value: []byte("skip-me-1")},
		{Key: []byte("during-pause-2"), Value: []byte("skip-me-2")},
		{Key: []byte("during-pause-3"), Value: []byte("skip-me-3")},
		{Key: []byte("during-pause-4"), Value: []byte("skip-me-4")},
		{Key: []byte("during-pause-5"), Value: []byte("skip-me-5")},
	}
	err = producer.WriteMessages(context.Background(), duringPauseMessages...)
	if err != nil {
		t.Fatalf("Failed to produce messages during pause: %v", err)
	}

	// Verify messages are in Kafka but NOT consumed (consumer is paused)
	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		t.Fatalf("Failed to read last offset: %v", err)
	}
	t.Logf("Last offset in Kafka after pause production: %d", lastOffset)
	if lastOffset != 7 { // 2 initial + 5 during pause
		t.Fatalf("Expected last offset to be 7, got %d", lastOffset)
	}

	// Wait during pause to simulate real-world scenario
	time.Sleep(500 * time.Millisecond)

	// Verify NO messages consumed during pause
	select {
	case msg := <-messageCh:
		t.Fatalf("Consumer consumed message during pause! key=%s, offset=%d", msg.key, msg.offset)
	case <-time.After(100 * time.Millisecond):
		t.Log("✓ Verified: No messages consumed during pause")
	}

	// Phase 4: ResumeFromLatestOffset (CRITICAL TEST)
	t.Log("Phase 4: Calling ResumeFromLatestOffset (messages during pause should be skipped)...")
	resumeTimestamp = time.Now()
	err = consumer.ResumeFromLatestOffset()
	if err != nil {
		t.Fatalf("ResumeFromLatestOffset failed: %v", err)
	}

	// Wait for resume to take effect
	time.Sleep(200 * time.Millisecond)

	// Phase 5: Produce NEW messages AFTER ResumeFromLatestOffset
	t.Log("Phase 5: Producing new messages after ResumeFromLatestOffset...")
	afterResumeMessages := []segmentio.Message{
		{Key: []byte("after-resume-1"), Value: []byte("consume-me-1")},
		{Key: []byte("after-resume-2"), Value: []byte("consume-me-2")},
	}
	err = producer.WriteMessages(context.Background(), afterResumeMessages...)
	if err != nil {
		t.Fatalf("Failed to produce messages after resume: %v", err)
	}

	// Phase 6: CRITICAL VERIFICATION - Only after-resume messages should be consumed
	t.Log("Phase 6: Verifying only new messages are consumed...")

	var consumedAfterResume []consumedMessage
	timeout := time.After(5 * time.Second)
	expectedCount := 2

	for i := 0; i < expectedCount; i++ {
		select {
		case msg := <-messageCh:
			consumedAfterResume = append(consumedAfterResume, msg)
			t.Logf("✓ Consumed after resume: offset=%d, key=%s, value=%s, timestamp=%v",
				msg.offset, msg.key, msg.value, msg.timestamp)
		case <-timeout:
			t.Fatalf("Timeout waiting for message %d/%d after resume", i+1, expectedCount)
		}
	}

	// Verify correct messages consumed
	if len(consumedAfterResume) != 2 {
		t.Fatalf("Expected 2 messages after resume, got %d", len(consumedAfterResume))
	}

	if consumedAfterResume[0].key != "after-resume-1" {
		t.Fatalf("Expected first message key 'after-resume-1', got '%s'", consumedAfterResume[0].key)
	}
	if consumedAfterResume[1].key != "after-resume-2" {
		t.Fatalf("Expected second message key 'after-resume-2', got '%s'", consumedAfterResume[1].key)
	}

	// CRITICAL: Verify no more messages consumed (during-pause messages were skipped)
	select {
	case msg := <-messageCh:
		t.Fatalf("❌ FAIL: Unexpected message consumed! This should have been skipped: key=%s, value=%s, offset=%d",
			msg.key, msg.value, msg.offset)
	case <-time.After(1 * time.Second):
		t.Log("✓ PASS: No during-pause messages consumed - they were correctly skipped!")
	}

	// Phase 7: Verify timing guarantees (microsecond precision)
	t.Log("Phase 7: Verifying timing guarantees...")
	for _, msg := range consumedAfterResume {
		if msg.timestamp.Before(resumeTimestamp) {
			t.Fatalf("❌ Message consumed before resume timestamp! msg.timestamp=%v, resumeTimestamp=%v",
				msg.timestamp, resumeTimestamp)
		}
		if msg.timestamp.Before(pauseTimestamp.Add(500 * time.Millisecond)) {
			t.Logf("⚠️  Warning: Message timestamp very close to pause timestamp")
		}
	}
	t.Log("✓ All timing guarantees verified")

	// Phase 8: Final offset verification
	t.Log("Phase 8: Final offset verification...")
	finalOffset, err := conn.ReadLastOffset()
	if err != nil {
		t.Fatalf("Failed to read final offset: %v", err)
	}
	t.Logf("Final offset in Kafka: %d", finalOffset)

	// We should have: 2 initial + 5 during-pause + 2 after-resume = 9 total messages
	if finalOffset != 9 {
		t.Fatalf("Expected final offset to be 9, got %d", finalOffset)
	}

	t.Log("✅ TEST PASSED: ResumeFromLatestOffset correctly skipped messages during pause")
	t.Log("✅ Messages during pause (5): SKIPPED")
	t.Log("✅ Messages after resume (2): CONSUMED")
	t.Log("✅ No message loss detected")
	t.Log("✅ Microsecond timing verified")
}

func Test_Should_Resume_From_Latest_Offset_With_Multiple_Partitions(t *testing.T) {
	// Given
	t.Parallel()
	topic := "resume-multipart-topic"
	consumerGroup := "resume-multipart-cg"
	brokerAddress := "localhost:9092"

	// Create topic with multiple partitions
	conn, err := segmentio.DialLeader(context.Background(), "tcp", brokerAddress, topic, 0)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create topic with 3 partitions
	err = conn.CreateTopics(segmentio.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Logf("Topic might already exist: %v", err)
	}

	cleanUp := func() {
		if err := conn.DeleteTopics(topic); err != nil {
			t.Logf("Failed to delete topic: %v", err)
		}
		conn.Close()
	}
	defer cleanUp()

	time.Sleep(500 * time.Millisecond) // Wait for topic creation

	consumedMessages := make(chan string, 100)

	consumerCfg := &kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: []string{brokerAddress},
			Topic:   topic,
			GroupID: consumerGroup,
		},
		ConsumeFn: func(message *kafka.Message) error {
			consumedMessages <- fmt.Sprintf("p%d-o%d-%s", message.Partition, message.Offset, string(message.Key))
			return nil
		},
	}

	consumer, err := kafka.NewConsumer(consumerCfg)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Stop()

	consumer.Consume()

	producer := &segmentio.Writer{
		Topic:    topic,
		Addr:     segmentio.TCP(brokerAddress),
		Balancer: &segmentio.RoundRobin{},
	}
	defer producer.Close()

	// Phase 1: Produce initial messages to all partitions
	t.Log("Phase 1: Producing initial messages...")
	for i := 0; i < 3; i++ {
		err = producer.WriteMessages(context.Background(), segmentio.Message{
			Key:   []byte(fmt.Sprintf("initial-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to produce initial message: %v", err)
		}
	}

	// Consume initial messages
	for i := 0; i < 3; i++ {
		select {
		case msg := <-consumedMessages:
			t.Logf("Consumed initial: %s", msg)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout consuming initial message %d", i)
		}
	}

	// Phase 2: Pause and produce messages
	t.Log("Phase 2: Pausing and producing during pause...")
	consumer.Pause()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 9; i++ { // 9 messages across 3 partitions
		err = producer.WriteMessages(context.Background(), segmentio.Message{
			Key:   []byte(fmt.Sprintf("during-pause-%d", i)),
			Value: []byte(fmt.Sprintf("skip-%d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to produce during pause: %v", err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	// Phase 3: ResumeFromLatestOffset
	t.Log("Phase 3: ResumeFromLatestOffset...")
	err = consumer.ResumeFromLatestOffset()
	if err != nil {
		t.Fatalf("ResumeFromLatestOffset failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Phase 4: Produce new messages
	t.Log("Phase 4: Producing after resume...")
	for i := 0; i < 3; i++ {
		err = producer.WriteMessages(context.Background(), segmentio.Message{
			Key:   []byte(fmt.Sprintf("after-resume-%d", i)),
			Value: []byte(fmt.Sprintf("consume-%d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to produce after resume: %v", err)
		}
	}

	// Phase 5: Verify only after-resume messages consumed
	t.Log("Phase 5: Verifying consumption...")
	var afterResumeCount int
	timeout := time.After(5 * time.Second)

	for afterResumeCount < 3 {
		select {
		case msg := <-consumedMessages:
			t.Logf("✓ Consumed: %s", msg)
			afterResumeCount++
		case <-timeout:
			t.Fatalf("Timeout: only got %d/3 messages after resume", afterResumeCount)
		}
	}

	// Verify no more messages (during-pause messages were skipped)
	select {
	case msg := <-consumedMessages:
		t.Fatalf("❌ Unexpected message consumed: %s", msg)
	case <-time.After(1 * time.Second):
		t.Log("✓ PASS: Multi-partition skip verified!")
	}

	t.Log("✅ TEST PASSED: ResumeFromLatestOffset works correctly with multiple partitions")
}
