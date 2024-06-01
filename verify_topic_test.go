package kafka

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"testing"
)

type mockKafkaClientWrapper struct {
	wantErr        bool
	wantExistTopic bool
}

func (m mockKafkaClientWrapper) GetClient() *kafka.Client {
	return &kafka.Client{}
}

func (m mockKafkaClientWrapper) Metadata(_ context.Context, _ *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	if m.wantErr {
		return nil, errors.New("metadataReqErr")
	}

	if !m.wantExistTopic {
		return &kafka.MetadataResponse{
			Topics: []kafka.Topic{
				{Name: "topic1", Error: kafka.UnknownTopicOrPartition},
				{Name: "topic2", Error: nil},
			},
		}, nil
	}

	return &kafka.MetadataResponse{
		Topics: []kafka.Topic{
			{Name: "topic1", Error: nil},
			{Name: "topic2", Error: nil},
		},
	}, nil
}

func Test_kafkaClientWrapper_VerifyTopics(t *testing.T) {
	t.Run("Should_Return_Error_When_Metadata_Request_Has_Failed", func(t *testing.T) {
		// Given
		mockClient := mockKafkaClientWrapper{wantErr: true}
		cfg := &ConsumerConfig{}

		// When
		_, err := verifyTopics(mockClient, cfg)

		// Then
		if err == nil {
			t.Error("metadata request must be failed!")
		}
	})
	t.Run("Should_Return_False_When_Given_Topic_Does_Not_Exist", func(t *testing.T) {
		// Given
		mockClient := mockKafkaClientWrapper{wantExistTopic: false}
		cfg := &ConsumerConfig{
			Reader: ReaderConfig{
				Topic: "topic1",
			},
		}

		// When
		exist, err := verifyTopics(mockClient, cfg)

		// Then
		if exist {
			t.Errorf("topic %s must not exist", cfg.Reader.Topic)
		}
		if err != nil {
			t.Error("err must be nil")
		}
	})
	t.Run("Should_Return_True_When_Given_Topic_Exist", func(t *testing.T) {
		// Given
		mockClient := mockKafkaClientWrapper{wantExistTopic: true}
		cfg := &ConsumerConfig{
			Reader: ReaderConfig{
				Topic: "topic1",
			},
		}

		// When
		exist, err := verifyTopics(mockClient, cfg)

		// Then
		if !exist {
			t.Errorf("topic %s must exist", cfg.Reader.Topic)
		}
		if err != nil {
			t.Error("err must be nil")
		}
	})
}

func Test_newKafkaClient(t *testing.T) {
	// Given
	cfg := &ConsumerConfig{
		Reader: ReaderConfig{
			Topic:   "topic",
			Brokers: []string{"127.0.0.1:9092"},
		},
	}

	// When
	client, err := newKafkaClient(cfg)

	// Then
	if client.GetClient().Addr.String() != "127.0.0.1:9092" {
		t.Errorf("broker address must be 127.0.0.1:9092")
	}
	if err != nil {
		t.Errorf("err must be nil")
	}
}

func Test_kClient_GetClient(t *testing.T) {
	// Given
	mockClient := mockKafkaClientWrapper{}

	// When
	client := mockClient.GetClient()

	// Then
	if client == nil {
		t.Error("client must not be nil")
	}
}
