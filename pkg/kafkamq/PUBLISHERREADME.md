# Kafka Publisher (Watermill Kafka)

Этот пакет предоставляет **долгоживущий** Kafka publisher поверх Watermill Kafka
(sarama), а также **реестр паблишеров**, чтобы модули могли удобно объявлять свои
паблишеры, а `KafkaKernel` — инициализировать их на старте.

---

## Зачем так сделано

### ✅ Правильный lifecycle
- Каждый publisher создаётся **один раз** (собственный sarama sync-producer) и
  переиспользуется на протяжении жизни приложения.
- На `Stop()` все publisher’ы корректно закрываются.

### ❌ Неправильный вариант
Создавать `kafka.NewPublisher(...)` на каждый `Publish()` — дорого (новое TCP
соединение и sarama-producer на каждое сообщение) и ломает батчинг/ретраи.

---

## Термины: что такое `topic`

В отличие от Rabbit, где `topic` — условное имя routing key поверх фиксированного
exchange, в Kafka `topic` — это topic Kafka **напрямую**:

```go
events.Publish("orders.paid", payload)
```

Kafka:
```
topic = "orders.paid"
```

---

## Быстрый старт

Включить publisher в Kernel:

```go
kafkaKernel := app.NewKafkaKernel().
    EnablePublisher()
```

---

## Объявление паблишеров

```go
pubReg.Register(config.NewPublisherDefinition(
    "events",
    config.NewPublisherConfig()...,
))

pubReg.Register(config.NewPublisherDefinition(
    "orders",
    config.NewPublisherWithPartitioningConfig(func(topic string, msg *message.Message) (string, error) {
        return msg.Metadata.Get("order_id"), nil // сохраняем порядок сообщений одного заказа
    })...,
))
```

- `events`, `orders` — имена паблишеров в registry

---

## Использование

```go
eventsPub, err := pubReg.Get("events")
if err != nil {
    return err
}

return eventsPub.Publish("orders.paid", payload)
```

С метаданными:

```go
eventsPub.PublishWithMetaData("orders.paid", map[string]string{
    "trace_id": traceID,
}, payload)
```

Batch:

```go
eventsPub.PublishBatch("products.updated", []any{p1, p2, p3})
```

---

## Partition key и порядок сообщений

Kafka гарантирует порядок сообщений **только в рамках одной партиции**. Без
явного partition key сообщения размазываются round-robin — используйте
`config.NewPublisherWithPartitioningConfig`, если порядок важен (например все
события одного заказа/агрегата должны попадать в одну партицию):

```go
config.NewPublisherWithPartitioningConfig(func(topic string, msg *message.Message) (string, error) {
    return msg.Metadata.Get("aggregate_id"), nil
})
```

---

## API

### Publisher
- `Publish(topic string, payload any) error`
- `PublishWithMetaData(topic string, meta map[string]string, payload any) error`
- `PublishBatch(topic string, payloads []any) error`
- `PublishBatchWithMetaData(topic string, meta map[string]string, payloads []any) error`
- `Close() error`

### PublisherRegistry
- `Register(def PublisherDefinition) error`
- `RegisterMultiple(list []PublisherDefinition) error`
- `Get(name string) (*Publisher, error)`
- `Close() error`

---

## Рекомендации по неймингу топиков

Используйте формат:

```
domain.event
```

Примеры:
- `orders.paid`
- `orders.created`
- `products.updated`
- `users.created`
