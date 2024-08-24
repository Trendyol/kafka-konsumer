package kafka

import "testing"

func TestSASLConfig_Json(t *testing.T) {
	t.Run("Should_Convert_Nil_Config_To_Json", func(t *testing.T) {
		// Given
		var cfg *SASLConfig

		expected := "{}"
		// When
		result := cfg.JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json", func(t *testing.T) {
		// Given
		cfg := &SASLConfig{
			Type:     "scram",
			Username: "user",
			Password: "pass",
		}

		expected := "{\"Mechanism\": \"scram\", \"Username\": \"user\", \"Password\": \"pass\"}"
		// When
		result := cfg.JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}
