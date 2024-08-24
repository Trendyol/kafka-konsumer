package kafka

import (
	"testing"
	"time"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/google/go-cmp/cmp"
)

func TestConsumerConfig_validate(t *testing.T) {
	t.Run("Set_Defaults", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{Reader: ReaderConfig{}}

		// When
		cfg.setDefaults()

		// Then
		if cfg.Concurrency != 1 {
			t.Fatalf("Concurrency default value must equal to 1")
		}
		if cfg.CommitInterval != time.Second {
			t.Fatalf("Commit Interval default value must equal to 1s")
		}
		if cfg.Reader.CommitInterval != time.Second {
			t.Fatalf("Reader Commit Interval default value must equal to 1s")
		}
		if *cfg.TransactionalRetry != true {
			t.Fatal("Default Transactional Retry is true")
		}
		if cfg.MessageGroupDuration != time.Second {
			t.Fatal("Message Group Duration default value must equal to 1s")
		}
		if cfg.BatchConfiguration != nil {
			t.Fatalf("Batch configuration not specified so it must stay as nil")
		}
	})
	t.Run("Set_Defaults_For_BatchConfiguration", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{BatchConfiguration: &BatchConfiguration{}}

		// When
		cfg.setDefaults()

		// Then

		if cfg.BatchConfiguration.MessageGroupLimit != 100 {
			t.Fatalf("MessageGroupLimit Batch configuration not specified so it must take default value")
		}
	})
	t.Run("Set_Defaults_When_Distributed_Tracing_Enabled", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{Reader: ReaderConfig{}, DistributedTracingEnabled: true}

		// When
		cfg.setDefaults()

		// Then
		if cfg.DistributedTracingConfiguration.TracerProvider == nil {
			t.Fatal("Traceprovider cannot be null")
		}
		if cfg.DistributedTracingConfiguration.Propagator == nil {
			t.Fatal("Propagator cannot be null")
		}
	})
	t.Run("Set_Commit_Interval_Value_To_The_Internal_Reader", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{CommitInterval: 5 * time.Second, Reader: ReaderConfig{}}

		// When
		cfg.setDefaults()

		// Then
		if cfg.CommitInterval != 5*time.Second {
			t.Fatalf("Commit Interval value must equal to 5s")
		}
		if cfg.Reader.CommitInterval != 5*time.Second {
			t.Fatalf("Reader Commit Interval default value must equal to 5s")
		}
	})
}

func TestConsumerConfig_newCronsumerConfig(t *testing.T) {
	t.Run("Should_Return_Nil_When_Client_Don't_Use_SkipMessageByHeaderFn", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{}

		// When
		actual := cfg.newCronsumerConfig()

		// Then
		if actual.Consumer.SkipMessageByHeaderFn != nil {
			t.Error("SkipMessageByHeaderFn must be nil")
		}
	})
	t.Run("Should_Set_When_Client_Give_SkipMessageByHeaderFn", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{
			RetryConfiguration: RetryConfiguration{
				SkipMessageByHeaderFn: func(_ []Header) bool {
					return false
				},
			},
		}

		// When
		actual := cfg.newCronsumerConfig()

		// Then
		if actual.Consumer.SkipMessageByHeaderFn == nil {
			t.Error("SkipMessageByHeaderFn mustn't be nil")
		}
	})
}

func Test_toHeader(t *testing.T) {
	t.Run("Should_Return_Empty_List_When_Cronsumer_Header_Is_Nil", func(t *testing.T) {
		// When
		headers := toHeaders(nil)

		// Then
		if len(headers) != 0 {
			t.Error("Header must be nil")
		}
	})
	t.Run("Should_Return_Empty_List_When_Cronsumer_Header_Is_Empty", func(t *testing.T) {
		// When
		headers := toHeaders([]kcronsumer.Header{})

		// Then
		if len(headers) != 0 {
			t.Error("Header must be nil")
		}
	})
	t.Run("Should_Covert_List_When_Cronsumer_Header", func(t *testing.T) {
		// Given
		expected := []Header{
			{Key: "key", Value: []byte("val")},
			{Key: "key2", Value: []byte("val2")},
			{Key: "key3", Value: nil},
		}

		// When
		actual := toHeaders([]kcronsumer.Header{
			{Key: "key", Value: []byte("val")},
			{Key: "key2", Value: []byte("val2")},
			{Key: "key3", Value: nil},
		})

		// Then
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
}

func TestConsumerConfig_getTopics(t *testing.T) {
	t.Run("Should_Get_Consumer_Group_Topics", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{
			Reader: ReaderConfig{
				GroupTopics: []string{"t1", "t2", "t3"},
			},
		}

		// When
		result := cfg.getTopics()

		// Then
		if len(result) != 3 {
			t.Error("len of result must be equal 3")
		}
	})
	t.Run("Should_Get_Topic", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{
			Reader: ReaderConfig{
				Topic: "t1",
			},
		}

		// When
		result := cfg.getTopics()

		// Then
		if len(result) != 1 {
			t.Error("len of result must be equal 1")
		}
	})
}

func Test_jsonPretty(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple JSON",
			input:    `{"key1":"value1","key2":2}`,
			expected: "{\n\t\"key1\": \"value1\",\n\t\"key2\": 2\n}",
		},
		{
			name:     "Nested JSON",
			input:    `{"key1":"value1","key2":{"nestedKey1":1,"nestedKey2":2},"key3":[1,2,3]}`,
			expected: "{\n\t\"key1\": \"value1\",\n\t\"key2\": {\n\t\t\"nestedKey1\": 1,\n\t\t\"nestedKey2\": 2\n\t},\n\t\"key3\": [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n}",
		},
		{
			name:     "Invalid JSON",
			input:    `{"key1": "value1", "key2": 2`,
			expected: `{"key1": "value1", "key2": 2`,
		},
		{
			name:     "Empty JSON",
			input:    ``,
			expected: ``,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jsonPretty(tt.input)
			if got != tt.expected {
				t.Errorf("jsonPretty() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConsumerConfig_JSON(t *testing.T) {
	t.Run("Should_Convert_Nil_Config_To_Json", func(t *testing.T) {
		// Given
		var config *ConsumerConfig
		expected := "{}"
		// When
		result := config.JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json", func(t *testing.T) {
		// Given
		expected := "{\"ClientID\": \"test-consumer-client-id\", \"Reader\": {\"Brokers\": [\"broker-1.test.com\", \"broker-2.test.com\"], " +
			"\"GroupId\": \"test-consumer.0\", \"GroupTopics\": [\"test-updated.0\"], \"MaxWait\": \"2s\", " +
			"\"CommitInterval\": \"1s\", \"StartOffset\": \"earliest\"}, \"BatchConfiguration\": {\"MessageGroupLimit\": 100}, " +
			"\"MessageGroupDuration\": \"20ns\", \"TransactionalRetry\": false, \"Concurrency\": 10, \"RetryEnabled\": true, " +
			"\"RetryConfiguration\": {\"Brokers\": [\"broker-1.test.com\", \"broker-2.test.com\"], \"Topic\": \"test-exception.0\", " +
			"\"StartTimeCron\": \"*/2 * * * *\", \"WorkDuration\": \"1m0s\", \"MaxRetry\": 3, \"VerifyTopicOnStartup\": true, \"Rack\": \"\"}, " +
			"\"VerifyTopicOnStartup\": true, \"Rack\": \"stage\", " +
			"\"SASL\": {\"Mechanism\": \"scram\", \"Username\": \"user\", \"Password\": \"pass\"}, " +
			"\"TLS\": {\"RootCAPath\": \"resources/ca\", \"IntermediateCAPath\": \"resources/intCa\"}}"
		// When
		result := getConsumerConfigExample().JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "{\"ClientID\": \"test-consumer-client-id\", \"Reader\": {\"Brokers\": [\"\"], \"GroupId\": \"\", " +
			"\"GroupTopics\": [\"\"], \"MaxWait\": \"0s\", \"CommitInterval\": \"0s\", \"StartOffset\": \"earliest\"}, " +
			"\"BatchConfiguration\": {}, \"MessageGroupDuration\": \"20ns\", \"TransactionalRetry\": false, \"Concurrency\": 10, " +
			"\"RetryEnabled\": true, \"RetryConfiguration\": {\"Brokers\": [\"\"], \"Topic\": \"\", \"StartTimeCron\": \"\", " +
			"\"WorkDuration\": \"0s\", \"MaxRetry\": 0, \"VerifyTopicOnStartup\": false, \"Rack\": \"\"}, \"VerifyTopicOnStartup\": true, " +
			"\"Rack\": \"stage\", \"SASL\": {}, \"TLS\": {}}"
		// When
		result := getConsumerConfigWithoutInnerObjectExample().JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func TestConsumerConfig_String(t *testing.T) {
	t.Run("Should_Convert_To_String", func(t *testing.T) {
		// Given
		expected := "ClientID: \"test-consumer-client-id\", Reader: {Brokers: [\"broker-1.test.com\", \"broker-2.test.com\"], " +
			"GroupId: \"test-consumer.0\", GroupTopics: [\"test-updated.0\"], MaxWait: \"2s\", CommitInterval: \"1s\", " +
			"StartOffset: \"earliest\"}, BatchConfiguration: {MessageGroupLimit: 100}, MessageGroupDuration: \"20ns\", " +
			"TransactionalRetry: false, Concurrency: 10, RetryEnabled: true, " +
			"RetryConfiguration: {Brokers: [\"broker-1.test.com\", \"broker-2.test.com\"], Topic: \"test-exception.0\", " +
			"StartTimeCron: \"*/2 * * * *\", WorkDuration: \"1m0s\", MaxRetry: 3, VerifyTopicOnStartup: true, Rack: \"\"}, " +
			"VerifyTopicOnStartup: true, Rack: \"stage\", SASL: {Mechanism: \"scram\", Username: \"user\", Password: \"pass\"}, " +
			"TLS: {RootCAPath: \"resources/ca\", IntermediateCAPath: \"resources/intCa\"}"
		// When
		result := getConsumerConfigExample().String()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_String_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "ClientID: \"test-consumer-client-id\", Reader: {Brokers: [\"\"], GroupId: \"\", " +
			"GroupTopics: [\"\"], MaxWait: \"0s\", CommitInterval: \"0s\", StartOffset: \"earliest\"}, " +
			"BatchConfiguration: {}, MessageGroupDuration: \"20ns\", TransactionalRetry: false, Concurrency: 10, " +
			"RetryEnabled: true, RetryConfiguration: {Brokers: [\"\"], Topic: \"\", StartTimeCron: \"\", WorkDuration: \"0s\", " +
			"MaxRetry: 0, VerifyTopicOnStartup: false, Rack: \"\"}, VerifyTopicOnStartup: true, Rack: \"stage\", SASL: {}, TLS: {}"
		// When
		result := getConsumerConfigWithoutInnerObjectExample().String()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func TestConsumerConfig_JSONPretty(t *testing.T) {
	t.Run("Should_Convert_To_Pretty_Json", func(t *testing.T) {
		// Given
		expected := "{\n\t\"ClientID\": \"test-consumer-client-id\",\n\t\"Reader\": {\n\t\t\"" +
			"Brokers\": [\n\t\t\t\"broker-1.test.com\",\n\t\t\t\"broker-2.test.com\"\n\t\t],\n\t\t\"" +
			"GroupId\": \"test-consumer.0\",\n\t\t\"GroupTopics\": [\n\t\t\t\"test-updated.0\"\n\t\t],\n\t\t\"" +
			"MaxWait\": \"2s\",\n\t\t\"CommitInterval\": \"1s\",\n\t\t\"StartOffset\": \"earliest\"\n\t},\n\t\"" +
			"BatchConfiguration\": {\n\t\t\"MessageGroupLimit\": 100\n\t},\n\t\"MessageGroupDuration\": \"20ns\",\n\t\"" +
			"TransactionalRetry\": false,\n\t\"Concurrency\": 10,\n\t\"RetryEnabled\": true,\n\t\"" +
			"RetryConfiguration\": {\n\t\t\"Brokers\": [\n\t\t\t\"broker-1.test.com\",\n\t\t\t\"broker-2.test.com\"\n\t\t],\n\t\t\"" +
			"Topic\": \"test-exception.0\",\n\t\t\"StartTimeCron\": \"*/2 * * * *\",\n\t\t\"WorkDuration\": \"1m0s\",\n\t\t\"" +
			"MaxRetry\": 3,\n\t\t\"VerifyTopicOnStartup\": true,\n\t\t\"Rack\": \"\"\n\t},\n\t\"" +
			"VerifyTopicOnStartup\": true,\n\t\"Rack\": \"stage\",\n\t\"" +
			"SASL\": {\n\t\t\"Mechanism\": \"scram\",\n\t\t\"Username\": \"user\",\n\t\t\"Password\": \"pass\"\n\t},\n\t\"" +
			"TLS\": {\n\t\t\"RootCAPath\": \"resources/ca\",\n\t\t\"IntermediateCAPath\": \"resources/intCa\"\n\t}\n}"
		// When
		result := getConsumerConfigExample().JSONPretty()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Pretty_Json_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "{\n\t\"ClientID\": \"test-consumer-client-id\",\n\t\"" +
			"Reader\": {\n\t\t\"Brokers\": [\n\t\t\t\"\"\n\t\t],\n\t\t\"GroupId\": \"\",\n\t\t\"" +
			"GroupTopics\": [\n\t\t\t\"\"\n\t\t],\n\t\t\"MaxWait\": \"0s\",\n\t\t\"CommitInterval\": \"0s\",\n\t\t\"" +
			"StartOffset\": \"earliest\"\n\t},\n\t\"BatchConfiguration\": {},\n\t\"" +
			"MessageGroupDuration\": \"20ns\",\n\t\"TransactionalRetry\": false,\n\t\"Concurrency\": 10,\n\t\"" +
			"RetryEnabled\": true,\n\t\"RetryConfiguration\": {\n\t\t\"Brokers\": [\n\t\t\t\"\"\n\t\t],\n\t\t\"" +
			"Topic\": \"\",\n\t\t\"StartTimeCron\": \"\",\n\t\t\"WorkDuration\": \"0s\",\n\t\t\"MaxRetry\": 0,\n\t\t\"" +
			"VerifyTopicOnStartup\": false,\n\t\t\"Rack\": \"\"\n\t},\n\t\"VerifyTopicOnStartup\": true,\n\t\"" +
			"Rack\": \"stage\",\n\t\"SASL\": {},\n\t\"TLS\": {}\n}"
		// When
		result := getConsumerConfigWithoutInnerObjectExample().JSONPretty()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func getConsumerConfigExample() *ConsumerConfig {
	return &ConsumerConfig{
		Rack:     "stage",
		ClientID: "test-consumer-client-id",
		Reader: ReaderConfig{
			Brokers:        []string{"broker-1.test.com", "broker-2.test.com"},
			GroupID:        "test-consumer.0",
			GroupTopics:    []string{"test-updated.0"},
			MaxWait:        2 * time.Second,
			CommitInterval: time.Second,
		},
		BatchConfiguration: &BatchConfiguration{
			MessageGroupLimit: 100,
		},
		MessageGroupDuration: 20,
		TransactionalRetry:   NewBoolPtr(false),
		Concurrency:          10,
		RetryEnabled:         true,
		RetryConfiguration: RetryConfiguration{
			Brokers:              []string{"broker-1.test.com", "broker-2.test.com"},
			Topic:                "test-exception.0",
			StartTimeCron:        "*/2 * * * *",
			WorkDuration:         time.Minute * 1,
			MaxRetry:             3,
			VerifyTopicOnStartup: true,
		},
		VerifyTopicOnStartup: true,
		TLS: &TLSConfig{
			RootCAPath:         "resources/ca",
			IntermediateCAPath: "resources/intCa",
		},
		SASL: &SASLConfig{
			Type:     "scram",
			Username: "user",
			Password: "pass",
		},
	}
}

func getConsumerConfigWithoutInnerObjectExample() *ConsumerConfig {
	return &ConsumerConfig{
		Rack:                 "stage",
		ClientID:             "test-consumer-client-id",
		Reader:               ReaderConfig{},
		MessageGroupDuration: 20,
		TransactionalRetry:   NewBoolPtr(false),
		Concurrency:          10,
		RetryEnabled:         true,
		RetryConfiguration:   RetryConfiguration{},
		VerifyTopicOnStartup: true,
	}
}
