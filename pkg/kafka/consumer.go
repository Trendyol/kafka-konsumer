package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/Abdulsametileri/kafka-template/pkg/config"
	"github.com/Abdulsametileri/kafka-template/pkg/listener"
	segmentio "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"io"
	"strings"
	"time"
)

type kafkaConsumer struct {
	consumer *segmentio.Reader
	logger   *zap.Logger
}

func NewConsumer(kafkaCfg *config.Kafka, kafkaConsumerCfg *config.Consumer) (listener.KafkaConsumer, error) {
	readerConfig := segmentio.ReaderConfig{
		Brokers:        strings.Split(kafkaCfg.Servers, ","),
		GroupID:        kafkaConsumerCfg.ConsumerGroup,
		GroupTopics:    kafkaConsumerCfg.Topics,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second, // flushes commits to Kafka every second
	}

	if kafkaCfg.IsSecureCluster {
		tls, err := newTLSConfig(kafkaCfg)
		if err != nil {
			return nil, err
		}

		saslMechanism, err := newMechanism(kafkaCfg)
		if err != nil {
			return nil, err
		}

		readerConfig.Dialer = &segmentio.Dialer{
			TLS:           tls,
			SASLMechanism: saslMechanism,
		}

		readerConfig.GroupBalancers = []segmentio.GroupBalancer{
			segmentio.RackAffinityGroupBalancer{Rack: kafkaCfg.Rack},
		}
	}

	return &kafkaConsumer{
		consumer: segmentio.NewReader(readerConfig),
	}, nil
}

func (k *kafkaConsumer) ReadMessage(ctx context.Context) (segmentio.Message, error) {
	msg, err := k.consumer.ReadMessage(ctx)
	if err != nil {
		consumerConfig := k.consumer.Config()
		if isReaderHasBeenClosed(err) {
			k.logger.Debug("Kafka ReadMessage Error", zap.Error(err), zap.String("topicName", consumerConfig.Topic))
			return msg, err
		}
		k.logger.Error(fmt.Sprintf("An error occurred while reading message. Consumer: %v, Topics: %v, Error: %v.", consumerConfig.GroupID, consumerConfig.GroupTopics, err))
	}
	return msg, err
}

func (k *kafkaConsumer) Stop() {
	err := k.consumer.Close()
	if err != nil {
		k.logger.Error("Error while closing kafka consumer: " + err.Error())
	}
}

func isReaderHasBeenClosed(err error) bool {
	return errors.Is(err, io.EOF)
}
