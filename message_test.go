package kafka

import (
	"bytes"
	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestMessage_Header(t *testing.T) {
	t.Run("When_Header_Exist", func(t *testing.T) {
		// Given
		m := Message{
			Headers: []kafka.Header{
				{Key: "foo", Value: []byte("fooValue")},
				{Key: "another", Value: []byte("anotherValue")},
			},
		}

		// When
		header := m.Header("foo")

		// Then
		if header.Key != "foo" {
			t.Fatalf("Header must be equal to foo")
		}
		if !bytes.Equal(header.Value, []byte("fooValue")) {
			t.Fatalf("Header value must be equal to fooValue")
		}
	})
	t.Run("When_Header_Does_Not_Exist", func(t *testing.T) {
		// Given
		m := Message{
			Headers: []kafka.Header{
				{Key: "foo", Value: []byte("fooValue")},
				{Key: "another", Value: []byte("anotherValue")},
			},
		}

		// When
		header := m.Header("notexist")

		// Then
		if header != nil {
			t.Fatalf("Header must be equal to nil")
		}
	})
}

func TestMessage_AddHeader(t *testing.T) {
	t.Run("When_New_Header_Comes", func(t *testing.T) {
		// Given
		m := Message{
			Headers: []kafka.Header{
				{Key: "foo", Value: []byte("fooValue")},
			},
		}

		// When
		m.AddHeader(kafka.Header{Key: "bar", Value: []byte("barValue")})

		// Then
		headers := m.Headers
		if len(headers) != 2 {
			t.Fatalf("Header length must be equal to 2")
		}
		if headers[1].Key != "bar" {
			t.Fatalf("Header key must be equal to bar")
		}
		if !bytes.Equal(headers[1].Value, []byte("barValue")) {
			t.Fatalf("Header value must be equal to barValue")
		}
	})
	t.Run("When_Same_Header_Comes", func(t *testing.T) {
		// Given
		m := Message{
			Headers: []kafka.Header{
				{Key: "foo", Value: []byte("fooValue")},
			},
		}

		// When
		m.AddHeader(kafka.Header{Key: "foo", Value: []byte("barValue")})

		// Then
		headers := m.Headers
		if len(headers) != 1 {
			t.Fatalf("Header length must be equal to 1")
		}
		if headers[0].Key != "foo" {
			t.Fatalf("Header key must be equal to foo")
		}
		if !bytes.Equal(headers[0].Value, []byte("barValue")) {
			t.Fatalf("Header value must be equal to barValue")
		}
	})
}

func TestMessage_RemoveHeader(t *testing.T) {
	// Given
	m := Message{
		Headers: []kafka.Header{
			{Key: "foo", Value: []byte("fooValue")},
		},
	}

	// When
	m.RemoveHeader(kafka.Header{Key: "foo", Value: []byte("fooValue")})

	// Then
	headers := m.Headers
	if len(headers) != 0 {
		t.Fatalf("Header length must be equal to 0")
	}
}

func TestMessage_toRetryableMessage(t *testing.T) {
	t.Run("When_error_description_exist", func(t *testing.T) {
		// Given
		message := Message{
			Key:   []byte("key"),
			Value: []byte("value"),
			Headers: []Header{
				{
					Key:   "x-custom-client-header",
					Value: []byte("bar"),
				},
			},
			ErrDescription: "some error description",
		}
		expected := kcronsumer.Message{
			Topic: "retry-topic",
			Key:   []byte("key"),
			Value: []byte("value"),
			Headers: []kcronsumer.Header{
				{
					Key:   "x-custom-client-header",
					Value: []byte("bar"),
				},
				{
					Key:   "x-error-message",
					Value: []byte("some error description"),
				},
			},
		}

		// When
		actual := message.toRetryableMessage("retry-topic", "consumeFn error")

		// Then
		if actual.Topic != expected.Topic {
			t.Errorf("topic must be %q", expected.Topic)
		}

		if !bytes.Equal(actual.Key, expected.Key) {
			t.Errorf("Key must be equal to %q", string(expected.Key))
		}

		if !bytes.Equal(actual.Value, expected.Value) {
			t.Errorf("Value must be equal to %q", string(expected.Value))
		}

		if len(actual.Headers) != 2 {
			t.Error("Header length must be equal to 2")
		}

		if actual.Headers[0].Key != expected.Headers[0].Key {
			t.Errorf("First Header key must be equal to %q", expected.Headers[0].Key)
		}

		if !bytes.Equal(actual.Headers[0].Value, expected.Headers[0].Value) {
			t.Errorf("First Header value must be equal to %q", expected.Headers[0].Value)
		}

		if actual.Headers[1].Key != expected.Headers[1].Key {
			t.Errorf("Second Header key must be equal to %q", expected.Headers[1].Key)
		}

		if !bytes.Equal(actual.Headers[1].Value, expected.Headers[1].Value) {
			t.Errorf("Second Header value must be equal to %q", expected.Headers[1].Value)
		}
	})
	t.Run("When_error_description_does_not_exist", func(t *testing.T) {
		// Given
		message := Message{
			Key:   []byte("key"),
			Value: []byte("value"),
			Headers: []Header{
				{
					Key:   "x-custom-client-header",
					Value: []byte("bar"),
				},
			},
		}
		expected := kcronsumer.Message{
			Topic: "retry-topic",
			Key:   []byte("key"),
			Value: []byte("value"),
			Headers: []kcronsumer.Header{
				{
					Key:   "x-custom-client-header",
					Value: []byte("bar"),
				},
				{
					Key:   "x-error-message",
					Value: []byte("consumeFn error"),
				},
			},
		}

		// When
		actual := message.toRetryableMessage("retry-topic", "consumeFn error")

		// Then
		if actual.Topic != expected.Topic {
			t.Errorf("topic must be %q", expected.Topic)
		}

		if !bytes.Equal(actual.Key, expected.Key) {
			t.Errorf("Key must be equal to %q", string(expected.Key))
		}

		if !bytes.Equal(actual.Value, expected.Value) {
			t.Errorf("Value must be equal to %q", string(expected.Value))
		}

		if len(actual.Headers) != 2 {
			t.Error("Header length must be equal to 2")
		}

		if actual.Headers[0].Key != expected.Headers[0].Key {
			t.Errorf("First Header key must be equal to %q", expected.Headers[0].Key)
		}

		if !bytes.Equal(actual.Headers[0].Value, expected.Headers[0].Value) {
			t.Errorf("First Header value must be equal to %q", expected.Headers[0].Value)
		}

		if actual.Headers[1].Key != expected.Headers[1].Key {
			t.Errorf("Second Header key must be equal to %q", expected.Headers[1].Key)
		}

		if !bytes.Equal(actual.Headers[1].Value, expected.Headers[1].Value) {
			t.Errorf("Second Header value must be equal to %q", expected.Headers[1].Value)
		}
	})
}
