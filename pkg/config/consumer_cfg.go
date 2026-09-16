package config

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Handler func(ctx context.Context, msg *message.Message) error

type ConsumeConfig struct {
	Handler  Handler
	Consumer *kafka.Subscriber
	// Topic - в отличие от rabbit-топологии (exchange/queue задаются на этапе конфигурации),
	// в Kafka топик передаётся явно в Subscribe, поэтому хранится отдельно от Config.
	Topic string
}

type ConsumerOption func(*kafka.SubscriberConfig)

// WithConsumerBrokers - переопределяет список брокеров (по умолчанию берётся из KafkaConfig)
func WithConsumerBrokers(brokers []string) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.Brokers = brokers
	}
}

// WithConsumerGroup - consumer group; если не задан, консьюмер читает все партиции топика
// без учёта group-committed offset.
func WithConsumerGroup(group string) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.ConsumerGroup = group
	}
}

// WithUnmarshaler - конфиг для анмаршалинга сообщений
func WithUnmarshaler(u kafka.DefaultMarshaler) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.Unmarshaler = u
	}
}

// WithCustomUnmarshaler - анмаршалинг через произвольную реализацию kafka.Unmarshaler
func WithCustomUnmarshaler(u kafka.Unmarshaler) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.Unmarshaler = u
	}
}

// WithConsumerSaramaConfig - полная замена sarama.Config для subscriber-а
// (offset initial, TLS, session timeout и т.п.)
func WithConsumerSaramaConfig(cfg *sarama.Config) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.OverwriteSaramaConfig = cfg
	}
}

// WithNackResendSleep - пауза перед повторной доставкой сообщения после Nack
func WithNackResendSleep(d time.Duration) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.NackResendSleep = d
	}
}

// WithReconnectRetrySleep - пауза перед реконнектом консьюмера после обрыва
func WithReconnectRetrySleep(d time.Duration) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.ReconnectRetrySleep = d
	}
}

// WithInitializeTopicDetails - топик будет создан (если не существует) при вызове
// Consumer.SubscribeInitialize/SubscribeInitializeWithContext.
func WithInitializeTopicDetails(td *sarama.TopicDetail) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.InitializeTopicDetails = td
	}
}

// WithOTELConsumer - включает/выключает OpenTelemetry трейсинг для consumer-а
func WithOTELConsumer(enabled bool) ConsumerOption {
	return func(config *kafka.SubscriberConfig) {
		config.OTELEnabled = enabled
	}
}
