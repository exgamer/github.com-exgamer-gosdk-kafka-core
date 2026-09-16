# gosdk-kafka-core

`gosdk-kafka-core` — Kafka kernel для приложений, построенных на базе `gosdk-core`.
Пакет инкапсулирует инициализацию consumer-а и publisher-а Kafka и подключается к
приложению как **kernel**. Архитектурно зеркалит `gosdk-rabbit-core` (те же
Kernel/Registry/DI паттерны), но поверх Watermill Kafka (sarama) вместо Watermill AMQP —
см. [отличия от gosdk-rabbit-core](#-отличия-от-gosdk-rabbit-core) ниже.

Основная цель — дать единый и предсказуемый способ поднятия Kafka-слоя поверх core SDK.

---

## 📦 Возможности

- 🌐 kafka kernel для `gosdk-core`
- 🚀 Инициализация kafka consumer (consumer group, offset policy)
- 🚀 Инициализация kafka publisher (с опциональным partition key)
- ⚙️ Конфигурация через config
- ♻️ Корректный shutdown
- 🧠 Интеграция с DI контейнером

---

## 🚀 Установка

```bash
go get github.com/exgamer/gosdk-kafka-core
```

---

[Что доступно в DI из коробки](pkg/di/container.go)

---

## 🧠 Концепция Kafka Kernel

Kafka kernel — это kernel приложения, который:
- регистрирует kafka зависимости в DI
- инициализирует kafka consumer
- инициализирует kafka publisher
- корректно завершает работу при shutdown

Kernel реализует интерфейс `KernelInterface` из `gosdk-core`.

---

## 🔌 Регистрация Kafka Kernel

```go
app.RegisterKernel(app.NewKafkaKernel().EnableConsumer().EnablePublisher()) // включаем консьюмер или паблишер в зависимости от необходимости
```

`EnableConsumer()`/`EnablePublisher()` — это не просто пометка "буду использовать": без
соответствующего вызова `ConsumersRegistry`/`PublisherRegistry` не попадают в DI, и
`di.GetKafkaConsumersRegistry`/`di.GetKafkaPublishersRegistry` вернут ошибку "not found".
`*config.KafkaConfig` при этом доступен в DI всегда — он нужен для `kafkamq.DeclareTopics`
даже когда ни consumer, ни publisher не подняты.

---

## ⚙️ Конфигурация

Kafka kernel использует конфигурацию из `pkg/config`.

Пример env-переменных:

```env
KAFKA_BROKERS=127.0.0.1:9092,127.0.0.1:9093
KAFKA_CONSUMER_GROUP=my-service
KAFKA_CLIENT_ID=my-service
```

`KAFKA_BROKERS` — список брокеров через запятую (см. `config.KafkaConfig.BrokersList`).
`KAFKA_CONSUMER_GROUP` — дефолтная consumer group для всех handler-ов, которые не задают
свою через `config.WithConsumerGroup`/`NewConsumerFromOldestConfig`. `KAFKA_CLIENT_ID` —
дефолтный sarama `ClientID` для publisher-а и consumer-а, если не переопределён через
`WithPublisherSaramaConfig`/`WithConsumerSaramaConfig`.

⚠️ Без consumer group (ни из `KAFKA_CONSUMER_GROUP`, ни из явной опции) Kafka не хранит
committed offset для подписчика: `msg.Ack()`/`msg.Nack()` ничего не коммитят, и после
каждого реконнекта сообщения будут вычитаны заново. `Consumer.RegisterHandler` в этом
случае пишет предупреждение в лог — но не запрещает такой режим (иногда он осознанно нужен,
например полное вычитывание compacted-топика).

---

## 🔧 Кастомный Marshaler и Sarama-конфиг

- `config.WithCustomMarshaler(m kafka.Marshaler)` / `config.WithCustomUnmarshaler(u kafka.Unmarshaler)` — своя реализация маршалинга (в отличие от `WithMarshaler`/`WithUnmarshaler`, принимающих только `kafka.DefaultMarshaler`).
- `config.WithPublisherSaramaConfig` / `config.WithConsumerSaramaConfig` — полная замена `*sarama.Config` (TLS, acks, retries, compression, session timeout и т.п.).
- `config.WithPartitioningMarshaler` — публикация с явным partition key, чтобы сохранить порядок сообщений в рамках одного ключа (см. [PUBLISHERREADME](pkg/kafkamq/PUBLISHERREADME.md)).

---

## 🗺️ Декларация топиков без consumer-а

Для топиков, у которых намеренно нет читателя в этом сервисе (например топик,
который только продюсируется, но требует нестандартных партиций/retention), не
нужно поднимать отдельный `sarama.NewClusterAdmin` в обход SDK:

```go
kafkaCfg, err := kafkaDi.GetKafkaConfig(a.Container)
if err != nil {
    return err
}

err = kafkamq.DeclareTopics(kafkaCfg.BrokersList(), nil, kafkamq.TopicSpec{
    Name:              "outbox-retry",
    NumPartitions:     6,
    ReplicationFactor: 3,
})
```

`kafkaDi.GetKafkaConfig` отдаёт тот же самый конфиг kernel-а (список брокеров), что
используется под consumer/publisher.

---

## 🧩 Работа с консьюмером

[Работа с консьюмером](pkg/kafkamq/CONSUMERREADME.md)


## 🧩 Работа с паблишером

---
[Работа с паблишером](pkg/kafkamq/PUBLISHERREADME.md)

---

## ♻️ Graceful Shutdown

Kafka kernel автоматически:
- отменяет контекст consumer-а
- закрывает все publisher-ы
- закрывает всех зарегистрированных subscriber-ов (sarama-клиентов)

---

## 🔄 Отличия от gosdk-rabbit-core

Kafka — не AMQP-брокер: нет exchange/queue/routing key, вместо них — topic +
partition + consumer group. Это меняет форму SDK в нескольких местах:

| gosdk-rabbit-core | gosdk-kafka-core | Причина |
|---|---|---|
| `pkg/rabbitmq` | `pkg/kafkamq` | имя пакета не пересекается с импортируемым `watermill-kafka/pkg/kafka`, как rabbit-core избегает конфликта с `pkg/amqp` |
| единый `config.Config` для consumer и publisher | раздельные `config.PublisherOption` / `config.ConsumerOption` | watermill-kafka использует разные структуры `PublisherConfig`/`SubscriberConfig`, в отличие от единого `amqp.Config` |
| `*amqp.ConnectionWrapper` — общее соединение в DI | `*config.KafkaConfig` — общий конфиг (брокеры) в DI | у Kafka нет постоянного соединения: каждый publisher/subscriber держит свой sarama-клиент |
| `enums.TopicExchange/DirectExchange/FanoutExchange` | `enums.OffsetOldest/OffsetNewest` | в Kafka нет типов exchange — есть начальный offset консьюмера |
| `rabbitmq.DeclareTopology` (declare на канале) | `kafkamq.DeclareTopics` (создание топиков через ClusterAdmin) | разные примитивы администрирования брокера |
| `Subscribe(ctx, "")` — topic не используется (задан в exchange/queue) | `Subscribe(ctx, topic)` — topic обязателен | в Kafka topic передаётся в Subscribe явно, поэтому `config.HandlerRegister`/`config.ConsumeConfig` хранят поле `Topic` |
| `Consumer` не требует явного `Close()` (шарит connection) | `Consumer.Close()` закрывает все subscriber-ы | каждый Kafka-subscriber держит собственный sarama-клиент и должен закрываться явно |
| `EnableConsumer()`/`EnablePublisher()` не влияют на регистрацию реестров в `Init()` (реестры создаются всегда) | `EnableConsumer()`/`EnablePublisher()` реально гейтят регистрацию `ConsumersRegistry`/`PublisherRegistry` в DI | в rabbit-core эти флаги нигде не читаются в `Init()` — оставлено как есть в исходном SDK, но в kafka-core исправлено, чтобы флаги не вводили в заблуждение |

---

## 📌 Используется вместе с

- `gosdk-core`
- internal business modules

---

## 📝 License

MIT или внутренняя лицензия компании
