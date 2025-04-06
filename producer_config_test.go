package kafka

import (
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestProducerConfig_setDefaults(t *testing.T) {
	t.Run("Should_Set_Default_DistributedTracing_Fields", func(t *testing.T) {
		// Given
		cfg := ProducerConfig{DistributedTracingEnabled: true}

		// When
		cfg.setDefaults()

		// Then
		if cfg.DistributedTracingConfiguration.TracerProvider == nil {
			t.Fatal("TracerProvider cannot be nil")
		}
		if cfg.DistributedTracingConfiguration.Propagator == nil {
			t.Fatal("Propagator cannot be nil")
		}
	})

	t.Run("Should_Set_Default_Balancer_When_Nil", func(t *testing.T) {
		// Given
		cfg := ProducerConfig{
			Writer: WriterConfig{
				Balancer: nil,
			},
		}

		// When
		cfg.setDefaults()

		// Then
		if cfg.Writer.Balancer == nil {
			t.Fatal("Balancer should not be nil")
		}

		_, ok := cfg.Writer.Balancer.(*DefaultBalancer)
		if !ok {
			t.Fatalf("Expected balancer to be of type *DefaultBalancer, but got %T", cfg.Writer.Balancer)
		}
	})
}

func TestProducerConfig_Json(t *testing.T) {
	t.Run("Should_Convert_Nil_Config_To_Json", func(t *testing.T) {
		// Given
		var config *ProducerConfig
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
		expected := "{\"Writer\": {\"Brokers\": [\"broker-1.test.com\", \"broker-2.test.com\"], " +
			"\"Balancer\": \"Hash\", \"Compression\": \"gzip\"}, \"ClientID\": \"test-consumer-client-id\", " +
			"\"DistributedTracingEnabled\": false, " +
			"\"SASL\": {\"Mechanism\": \"scram\", \"Username\": \"user\", \"Password\": \"pass\"}, " +
			"\"TLS\": {\"RootCAPath\": \"resources/ca\", \"IntermediateCAPath\": \"resources/intCa\"}}"
		// When
		result := getProducerConfigExample().JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "{\"Writer\": {\"Brokers\": [\"\"], \"Balancer\": \"Unknown\", \"Compression\": \"uncompressed\"}, " +
			"\"ClientID\": \"test-consumer-client-id\", \"DistributedTracingEnabled\": false, \"SASL\": {}, \"TLS\": {}}"
		// When
		result := getProducerConfigWithoutInnerObjectExample().JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func TestProducerConfig_JsonPretty(t *testing.T) {
	t.Run("Should_Convert_To_Pretty_Json", func(t *testing.T) {
		// Given
		expected := "{\n\t\"Writer\": {\n\t\t\"Brokers\": [\n\t\t\t\"broker-1.test.com\",\n\t\t\t\"broker-2.test.com\"\n\t\t],\n\t\t\"" +
			"Balancer\": \"Hash\",\n\t\t\"Compression\": \"gzip\"\n\t},\n\t\"ClientID\": \"test-consumer-client-id\",\n\t\"" +
			"DistributedTracingEnabled\": false,\n\t\"" +
			"SASL\": {\n\t\t\"Mechanism\": \"scram\",\n\t\t\"Username\": \"user\",\n\t\t\"Password\": \"pass\"\n\t},\n\t\"" +
			"TLS\": {\n\t\t\"RootCAPath\": \"resources/ca\",\n\t\t\"IntermediateCAPath\": \"resources/intCa\"\n\t}\n}"
		// When
		result := getProducerConfigExample().JSONPretty()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Pretty_Json_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "{\n\t\"Writer\": {\n\t\t\"Brokers\": [\n\t\t\t\"\"\n\t\t],\n\t\t\"Balancer\": \"Unknown\",\n\t\t\"" +
			"Compression\": \"uncompressed\"\n\t},\n\t\"ClientID\": \"test-consumer-client-id\",\n\t\"" +
			"DistributedTracingEnabled\": false,\n\t\"SASL\": {},\n\t\"TLS\": {}\n}"
		// When
		result := getProducerConfigWithoutInnerObjectExample().JSONPretty()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}

func TestProducerConfig_removeSpaceBrokerList(t *testing.T) {
	type fields struct {
		Brokers []string
	}
	tests := []struct {
		name     string
		fields   fields
		expected []string
	}{
		{
			name: "Should_Remove_Spaces_In_Broker_Lists",
			fields: fields{
				Brokers: []string{" address", "address ", " address "},
			},
			expected: []string{"address", "address", "address"},
		},
		{
			name: "Should_Do_Nothing_When_Broker_Lists_Have_Not_Any_Space",
			fields: fields{
				Brokers: []string{"address1", "address2", "address3"},
			},
			expected: []string{"address1", "address2", "address3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			cfg := &WriterConfig{
				Brokers: tt.fields.Brokers,
			}

			// When
			cfg.removeSpaceBrokerList()

			// Then
			if !reflect.DeepEqual(cfg.Brokers, tt.expected) {
				t.Errorf("For broker list %v, expected %v", cfg.Brokers, tt.expected)
			}
		})
	}
}

func TestProducerConfig_String(t *testing.T) {
	t.Run("Should_Convert_To_String", func(t *testing.T) {
		// Given
		expected := "Writer: {Brokers: [\"broker-1.test.com\", \"broker-2.test.com\"], " +
			"Balancer: \"Hash\", Compression: \"gzip\"}, ClientID: \"test-consumer-client-id\", " +
			"DistributedTracingEnabled: false, SASL: {Mechanism: \"scram\", Username: \"user\", Password: \"pass\"}, " +
			"TLS: {RootCAPath: \"resources/ca\", IntermediateCAPath: \"resources/intCa\"}"
		// When
		result := getProducerConfigExample().String()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_String_Without_Inner_Object", func(t *testing.T) {
		// Given
		expected := "Writer: {Brokers: [\"\"], Balancer: \"Unknown\", Compression: \"uncompressed\"}, " +
			"ClientID: \"test-consumer-client-id\", DistributedTracingEnabled: false, SASL: {}, TLS: {}"
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
