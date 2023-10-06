package kafka

import "testing"

func Test_setDefaults(t *testing.T) {
	// Given
	cfg := ConsumerConfig{}

	// When
	setDefaults(&cfg)

	// Then
	if *cfg.APIConfiguration.Port != 8090 {
		t.Fatal("Default API Port is 8090")
	}
	if *cfg.APIConfiguration.HealthCheckPath != "/healthcheck" {
		t.Fatal("Default Healtcheck path is /healthcheck")
	}
	if *cfg.MetricConfiguration.Path != "/metrics" {
		t.Fatal("Default Healtcheck path is /metrics")
	}
}
