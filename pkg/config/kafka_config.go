package config

import "strings"

// KafkaConfig Данные для соединения с Kafka
type KafkaConfig struct {
	Brokers string `mapstructure:"KAFKA_BROKERS" json:"KAFKA_BROKERS"`
	// ConsumerGroup - дефолтная consumer group для kafkamq.Consumer.RegisterHandler.
	// Переопределяется точечно через config.WithConsumerGroup/NewConsumerFromOldestConfig
	// на конкретном handler-е.
	ConsumerGroup string `mapstructure:"KAFKA_CONSUMER_GROUP" json:"KAFKA_CONSUMER_GROUP"`
	// ClientID - дефолтный sarama ClientID для publisher-а и consumer-а. Переопределяется
	// точечно через config.WithPublisherSaramaConfig/WithConsumerSaramaConfig.
	ClientID string `mapstructure:"KAFKA_CLIENT_ID" json:"KAFKA_CLIENT_ID"`
}

// BrokersList - разбирает Brokers (через запятую) в слайс адресов для sarama/watermill-kafka.
func (c KafkaConfig) BrokersList() []string {
	parts := strings.Split(c.Brokers, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}

	return out
}
