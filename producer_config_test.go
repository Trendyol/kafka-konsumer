package kafka

import (
	"github.com/segmentio/kafka-go"
	"testing"
)

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

func TestProducerConfig_Json(t *testing.T) {
	t.Run("Should_Convert_Nil_Config_To_Json", func(t *testing.T) {
		// Given
		var config *ProducerConfig
		expected := "{}"
		// When
		result := config.Json()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json", func(t *testing.T) {
		// Given
		expected := "{\"Writer\": {\"Brokers\": [\"broker-1.test.com\", \"broker-2.test.com\"], \"Balancer\": \"Hash\", \"Compression\": \"gzip\"}, \"ClientID\": \"test-consumer-client-id\", \"DistributedTracingEnabled\": false, \"SASL\": {\"Mechanism\": \"scram\", \"Username\": \"user\", \"Password\": \"pass\"}, \"TLS\": {\"RootCAPath\": \"resources/ca\", \"IntermediateCAPath\": \"resources/intCa\"}}"
		// When
		result := getProducerConfigExample().Json()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "{\"Writer\": {\"Brokers\": [\"\"], \"Balancer\": \"Unknown\", \"Compression\": \"uncompressed\"}, \"ClientID\": \"test-consumer-client-id\", \"DistributedTracingEnabled\": false, \"SASL\": {}, \"TLS\": {}}"
		// When
		result := getProducerConfigWithoutInnerObjectExample().Json()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func TestProducerConfig_JsonPretty(t *testing.T) {
	t.Run("Should_Convert_To_Pretty_Json", func(t *testing.T) {
		// Given
		expected := "{\n\t\"Writer\": {\n\t\t\"Brokers\": [\n\t\t\t\"broker-1.test.com\",\n\t\t\t\"broker-2.test.com\"\n\t\t],\n\t\t\"Balancer\": \"Hash\",\n\t\t\"Compression\": \"gzip\"\n\t},\n\t\"ClientID\": \"test-consumer-client-id\",\n\t\"DistributedTracingEnabled\": false,\n\t\"SASL\": {\n\t\t\"Mechanism\": \"scram\",\n\t\t\"Username\": \"user\",\n\t\t\"Password\": \"pass\"\n\t},\n\t\"TLS\": {\n\t\t\"RootCAPath\": \"resources/ca\",\n\t\t\"IntermediateCAPath\": \"resources/intCa\"\n\t}\n}"
		// When
		result := getProducerConfigExample().JsonPretty()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Pretty_Json_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "{\n\t\"Writer\": {\n\t\t\"Brokers\": [\n\t\t\t\"\"\n\t\t],\n\t\t\"Balancer\": \"Unknown\",\n\t\t\"Compression\": \"uncompressed\"\n\t},\n\t\"ClientID\": \"test-consumer-client-id\",\n\t\"DistributedTracingEnabled\": false,\n\t\"SASL\": {},\n\t\"TLS\": {}\n}"
		// When
		result := getProducerConfigWithoutInnerObjectExample().JsonPretty()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func TestProducerConfig_String(t *testing.T) {
	t.Run("Should_Convert_To_String", func(t *testing.T) {
		// Given
		expected := "Writer: {Brokers: [\"broker-1.test.com\", \"broker-2.test.com\"], Balancer: \"Hash\", Compression: \"gzip\"}, ClientID: \"test-consumer-client-id\", DistributedTracingEnabled: false, SASL: {Mechanism: \"scram\", Username: \"user\", Password: \"pass\"}, TLS: {RootCAPath: \"resources/ca\", IntermediateCAPath: \"resources/intCa\"}"
		// When
		result := getProducerConfigExample().String()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_String_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "Writer: {Brokers: [\"\"], Balancer: \"Unknown\", Compression: \"uncompressed\"}, ClientID: \"test-consumer-client-id\", DistributedTracingEnabled: false, SASL: {}, TLS: {}"
		// When
		result := getProducerConfigWithoutInnerObjectExample().String()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func getProducerConfigExample() *ProducerConfig {
	return &ProducerConfig{
		ClientID: "test-consumer-client-id",
		Writer: WriterConfig{
			Balancer:    GetBalancerHash(),
			Brokers:     []string{"broker-1.test.com", "broker-2.test.com"},
			Compression: kafka.Gzip,
		},
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

func getProducerConfigWithoutInnerObjectExample() *ProducerConfig {
	return &ProducerConfig{
		ClientID: "test-consumer-client-id",
	}
}
