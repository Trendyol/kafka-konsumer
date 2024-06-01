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
				SkipMessageByHeaderFn: func(headers []Header) bool {
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
