package kafkamq

import (
	"fmt"
	"strings"

	"github.com/IBM/sarama"
)

// TopicSpec - описание топика для декларативного создания через DeclareTopics.
type TopicSpec struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	ConfigEntries     map[string]*string
}

// DeclareTopics открывает ClusterAdmin поверх брокеров kernel-а и создаёт топики —
// без необходимости поднимать собственный consumer или publisher.
//
// Нужен для топиков, у которых намеренно нет читателя в этом сервисе, либо когда
// нужно гарантировать наличие топика (партиции, replication factor, retention) до
// старта consumer/publisher. Аналог rabbitmq.DeclareTopology для мира Kafka.
//
// Уже существующий топик не считается ошибкой (idempotent declare). ClusterAdmin
// закрывается автоматически перед возвратом, независимо от результата.
func DeclareTopics(brokers []string, saramaConfig *sarama.Config, specs ...TopicSpec) error {
	// Валидируем все спеки до открытия соединения с брокером — чтобы конфигурационная
	// ошибка (забытый NumPartitions/ReplicationFactor) не пряталась за сетевым таймаутом
	// и не проверялась частично, если она в специфике, идущей после уже созданных топиков.
	for _, spec := range specs {
		if spec.Name == "" {
			return fmt.Errorf("declare topic: name is empty")
		}
		if spec.NumPartitions <= 0 {
			return fmt.Errorf("declare topic %q: NumPartitions must be > 0, got %d", spec.Name, spec.NumPartitions)
		}
		if spec.ReplicationFactor <= 0 {
			return fmt.Errorf("declare topic %q: ReplicationFactor must be > 0, got %d", spec.Name, spec.ReplicationFactor)
		}
	}

	if saramaConfig == nil {
		saramaConfig = sarama.NewConfig()
	}

	admin, err := sarama.NewClusterAdmin(brokers, saramaConfig)
	if err != nil {
		return fmt.Errorf("create cluster admin: %w", err)
	}
	defer admin.Close()

	for _, spec := range specs {
		detail := &sarama.TopicDetail{
			NumPartitions:     spec.NumPartitions,
			ReplicationFactor: spec.ReplicationFactor,
			ConfigEntries:     spec.ConfigEntries,
		}

		if err := admin.CreateTopic(spec.Name, detail, false); err != nil {
			if !strings.Contains(err.Error(), "Topic with this name already exists") {
				return fmt.Errorf("declare topic %q: %w", spec.Name, err)
			}
		}
	}

	return nil
}
