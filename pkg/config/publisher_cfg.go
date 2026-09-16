package config

import (
	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
)

type PublisherOption func(*kafka.PublisherConfig)

// WithPublisherBrokers - переопределяет список брокеров (по умолчанию берётся из KafkaConfig)
func WithPublisherBrokers(brokers []string) PublisherOption {
	return func(config *kafka.PublisherConfig) {
		config.Brokers = brokers
	}
}

// WithMarshaler - конфиг для маршалинга сообщений (дефолтный JSON/byte marshaler watermill-kafka)
func WithMarshaler(m kafka.DefaultMarshaler) PublisherOption {
	return func(config *kafka.PublisherConfig) {
		config.Marshaler = m
	}
}

// WithCustomMarshaler - конфиг для маршалинга сообщений через произвольную реализацию kafka.Marshaler
// (в отличие от WithMarshaler, принимающего только конкретный тип kafka.DefaultMarshaler)
func WithCustomMarshaler(m kafka.Marshaler) PublisherOption {
	return func(config *kafka.PublisherConfig) {
		config.Marshaler = m
	}
}

// WithPartitioningMarshaler - публикация с явным partition key (важно для сохранения
// порядка сообщений в рамках одного ключа, например agregate id).
func WithPartitioningMarshaler(generateKey kafka.GeneratePartitionKey) PublisherOption {
	return func(config *kafka.PublisherConfig) {
		config.Marshaler = kafka.NewWithPartitioningMarshaler(generateKey)
	}
}

// WithPublisherSaramaConfig - полная замена sarama.Config для publisher-а
// (acks, retries, compression, TLS и т.п.)
func WithPublisherSaramaConfig(cfg *sarama.Config) PublisherOption {
	return func(config *kafka.PublisherConfig) {
		config.OverwriteSaramaConfig = cfg
	}
}

// WithOTELPublisher - включает/выключает OpenTelemetry трейсинг для publisher-а
func WithOTELPublisher(enabled bool) PublisherOption {
	return func(config *kafka.PublisherConfig) {
		config.OTELEnabled = enabled
	}
}
