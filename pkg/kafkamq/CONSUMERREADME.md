## 🧩 Работа с консьюмером

Консьюмеры регистрируются в DI и доступны в бизнес-модулях.

```go
package consumers

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
)

func NewCityConsumer() *CityConsumer {
	return &CityConsumer{}
}

type CityConsumer struct {
}

func (c *CityConsumer) Consume(ctx context.Context, msg *message.Message) error {
	fmt.Println(string(msg.Payload))

	return nil
}
```

```go
package city

import (
	"github.com/exgamer/kafka-consumer-template/internal/domains/handbbok/modules/city/factories"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

func GetConsumers(
	consumersFactory *factories.CityConsumersFactory,
) []config.HandlerRegister {
	return []config.HandlerRegister{
		{
			Handler: consumersFactory.CityConsumer.Consume,
			Topic:   "cities",
			Config:  config.NewConsumerFromOldestConfig("city-service"),
		},
	}
}
```

```go
consumersFactory := factories.NewCityConsumersFactory()

consumers := GetConsumers(consumersFactory)

reg, err := di.GetKafkaConsumersRegistry(a.Container) // твой helper для DI
if err != nil {
    return err
}

reg.RegisterMultipleHandler(consumers)
```

---

## Consumer Group и offset

В отличие от Rabbit (где логика доставки определяется exchange/queue), в Kafka
поведение консьюмера определяется двумя вещами:

- **Consumer Group** (`config.WithConsumerGroup`) — несколько инстансов сервиса
  с одинаковой группой делят партиции топика между собой (горизонтальное
  масштабирование, каждое сообщение обрабатывается ровно одним инстансом).
- **Initial offset** (`config.NewConsumerFromOldestConfig` / `NewConsumerFromNewestConfig`) —
  откуда читать при первом запуске группы (когда ещё нет закоммиченного offset).

```go
config.NewConsumerFromOldestConfig("city-service") // не терять историю при первом деплое
config.NewConsumerFromNewestConfig("live-metrics")  // забирать только новые события
```

Если конкретный handler не передаёт свою группу через `Config`, используется дефолтная
`KAFKA_CONSUMER_GROUP` из env (`KafkaConfig.ConsumerGroup`). Если группа не задана ни там,
ни там — `RegisterHandler` всё равно зарегистрирует handler, но напишет в лог
предупреждение: **без consumer group `msg.Ack()`/`msg.Nack()` не коммитят offset**, и
сообщения будут вычитываться заново при каждом реконнекте/рестарте. Это осознанный режим
для отдельных сценариев (полное вычитывание compacted-топика и т.п.), но почти всегда
означает забытую группу.

---

## Ack / Nack

Как и в Rabbit, `msg.Ack()` коммитит offset, `msg.Nack()` вызывает повторную
доставку (после паузы `config.WithNackResendSleep`, дефолт 100ms). Политика по
умолчанию — `ActionNack` и для ошибок, и для паники в handler-е:

```go
consumer.WithErrorAction(kafkamq.ActionAck) // не ретраить бизнес-ошибки
consumer.WithPanicAction(kafkamq.ActionNack)
```
