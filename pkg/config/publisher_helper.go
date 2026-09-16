package config

import (
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
)

// NewPublisherConfig - дефолтный конфиг publisher-а (JSON payload + UUID header, без partition key).
// Топик передаётся на каждый Publish() отдельно (см. Publisher.Publish).
func NewPublisherConfig() []PublisherOption {
	return []PublisherOption{
		WithMarshaler(kafka.DefaultMarshaler{}),
	}
}

// NewPublisherWithPartitioningConfig - publisher с явным partition key. Важно для
// сохранения порядка сообщений в рамках одного ключа (например aggregate id) —
// без ключа Kafka размазывает сообщения по партициям round-robin.
func NewPublisherWithPartitioningConfig(generateKey kafka.GeneratePartitionKey) []PublisherOption {
	return []PublisherOption{
		WithPartitioningMarshaler(generateKey),
	}
}
