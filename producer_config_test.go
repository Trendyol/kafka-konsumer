package kafka

import "testing"

func TestProducerConfig_setDefaults(t *testing.T) {
	// Given
	cfg := ProducerConfig{DistributedTracingEnabled: true}

	// When
	cfg.setDefaults()

	// Then
	if cfg.DistributedTracingConfiguration.TracerProvider == nil {
		t.Fatal("Traceprovider cannot be null")
	}
	if cfg.DistributedTracingConfiguration.Propagator == nil {
		t.Fatal("Propagator cannot be null")
	}
}
