package config

import (
	config2 "github.com/exgamer/gosdk-core/pkg/config"
)

// InitKafkaConfig Инициализация конфига
func InitKafkaConfig() (*KafkaConfig, error) {
	kafkaConfig := &KafkaConfig{}
	err := config2.InitConfig(kafkaConfig)

	if err != nil {
		return nil, err
	}

	return kafkaConfig, nil
}
