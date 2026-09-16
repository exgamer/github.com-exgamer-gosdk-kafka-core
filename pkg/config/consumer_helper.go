package config

import (
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/exgamer/gosdk-kafka-core/pkg/enums"
)

// NewConsumerGroupConfig - дефолтный конфиг консьюмера с consumer group и заданным
// начальным offset (используется только при отсутствии закоммиченного offset для группы).
func NewConsumerGroupConfig(consumerGroup string, offsetInitial int64) []ConsumerOption {
	saramaCfg := kafka.DefaultSaramaSubscriberConfig()
	saramaCfg.Consumer.Offsets.Initial = offsetInitial

	return []ConsumerOption{
		WithConsumerGroup(consumerGroup),
		WithConsumerSaramaConfig(saramaCfg),
	}
}

// NewConsumerFromOldestConfig - консьюмер читает с начала топика при отсутствии
// закоммиченного offset (аналог "durable" консьюмера в очередях: ничего не теряется).
func NewConsumerFromOldestConfig(consumerGroup string) []ConsumerOption {
	return NewConsumerGroupConfig(consumerGroup, enums.OffsetOldest)
}

// NewConsumerFromNewestConfig - консьюмер читает только новые сообщения, историю топика
// не забирает (полезно для "живых" событий/метрик, где нет смысла разгребать бэклог).
func NewConsumerFromNewestConfig(consumerGroup string) []ConsumerOption {
	return NewConsumerGroupConfig(consumerGroup, enums.OffsetNewest)
}
