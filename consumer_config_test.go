package kafka

import (
	"testing"
	"time"
)

func TestConsumerConfig_validate(t *testing.T) {
	t.Run("Set_Defaults", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{Reader: ReaderConfig{}}

		// When
		cfg.validate()

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
	})
	t.Run("Set_Defaults_When_Distributed_Tracing_Enabled", func(t *testing.T) {
		// Given
		cfg := ConsumerConfig{Reader: ReaderConfig{}, DistributedTracingEnabled: true}

		// When
		cfg.validate()

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
		cfg.validate()

		// Then
		if cfg.CommitInterval != 5*time.Second {
			t.Fatalf("Commit Interval value must equal to 5s")
		}
		if cfg.Reader.CommitInterval != 5*time.Second {
			t.Fatalf("Reader Commit Interval default value must equal to 5s")
		}
	})
}
