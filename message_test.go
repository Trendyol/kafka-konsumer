package kafka

import (
	"bytes"
	"errors"
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

func TestMessage_CreateErrHeader(t *testing.T) {
	// Given
	e := errors.New("err")

	// When
	h := CreateErrHeader(e)

	// Then
	if h.Key != "X-ErrMessage" {
		t.Fatalf("Header key must be equal to X-ErrMessage")
	}

	if h.Value == nil {
		t.Fatalf("Header value must be present")
	}
}
