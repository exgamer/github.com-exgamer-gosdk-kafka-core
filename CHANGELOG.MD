# Changelog

## v0.1.0

- Первая версия `gosdk-kafka-core`: Kafka kernel по образцу `gosdk-rabbit-core`, поверх `ThreeDotsLabs/watermill-kafka` (sarama).
- `pkg/app`: `KafkaKernel`, `ConsumersRegistry`, `PublisherRegistry`.
- `pkg/kafkamq`: `Consumer` (consumer group, Ack/Nack, panic-safe, sentry-репортинг через `errorreporter`), `Publisher` (Publish/PublishBatch/PublishWithMetaData), `DeclareTopics`.
- `pkg/config`: `KafkaConfig` (env), функциональные опции `PublisherOption`/`ConsumerOption`, хелперы `NewConsumerFromOldestConfig`/`NewConsumerFromNewestConfig`, `NewPublisherConfig`/`NewPublisherWithPartitioningConfig`.
- `pkg/di`: хелперы `GetKafkaClient`, `GetKafkaConfig`, `GetKafkaConsumersRegistry`, `GetKafkaPublishersRegistry`.

### Правки по итогам ревью (до релиза)

- `KafkaConfig.ConsumerGroup`/`ClientID` теперь реально используются как дефолты в `kafkamq.Consumer.RegisterHandler`/`kafkamq.NewPublisher` (раньше были мёртвыми полями, только читались из env и нигде не применялись).
- `Consumer.RegisterHandler` пишет предупреждение в лог, если у handler-а нет consumer group (ни из env, ни из опции) — без группы `Ack`/`Nack` не коммитят offset.
- `KafkaKernel.EnableConsumer()`/`EnablePublisher()` теперь реально гейтят регистрацию `ConsumersRegistry`/`PublisherRegistry` в DI (раньше флаги ни на что не влияли в `Init()`, как и в `gosdk-rabbit-core`, откуда они унаследованы).
- `kafkamq.Consumer.handlers` защищён мьютексом (`RegisterHandler`/`Consume`/`Close` больше не гоняют один и тот же срез без синхронизации).
- `config.WithMarshaller` переименован в `config.WithMarshaler` для консистентности с `WithUnmarshaler`/`WithCustomMarshaler`/`WithCustomUnmarshaler`.
- `kafkamq.DeclareTopics` валидирует `TopicSpec` (`Name`/`NumPartitions`/`ReplicationFactor`) до открытия соединения с брокером, вместо того чтобы возвращать сырую ошибку брокера.
