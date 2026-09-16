package di

import (
	"github.com/exgamer/gosdk-core/pkg/di"
	"github.com/exgamer/gosdk-kafka-core/pkg/app"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
	"github.com/exgamer/gosdk-kafka-core/pkg/kafkamq"
)

// GetKafkaClient возвращает клиент-консьюмер kafka
func GetKafkaClient(c *di.Container) (*kafkamq.Consumer, error) {
	client, err := di.Resolve[*kafkamq.Consumer](c)

	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetKafkaConfig возвращает конфиг kernel-а. Нужен, когда требуется декларация
// топиков без собственного consumer-а или publisher-а — см. kafkamq.DeclareTopics.
func GetKafkaConfig(c *di.Container) (*config.KafkaConfig, error) {
	cfg, err := di.Resolve[*config.KafkaConfig](c)

	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// GetKafkaConsumersRegistry возвращает реестр консьюмеров
func GetKafkaConsumersRegistry(c *di.Container) (*app.ConsumersRegistry, error) {
	client, err := di.Resolve[*app.ConsumersRegistry](c)

	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetKafkaPublishersRegistry возвращает реестр publishers
func GetKafkaPublishersRegistry(c *di.Container) (*app.PublisherRegistry, error) {
	client, err := di.Resolve[*app.PublisherRegistry](c)

	if err != nil {
		return nil, err
	}

	return client, nil
}
