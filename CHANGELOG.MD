# Changelog

## v1.1.0

Зависимость `gosdk-core` поднята до v1.1.0. API пакета не изменился.

### Изменено
- `gosdk-core` v1.0.29 → v1.1.0.

### ⚠️ Изменения поведения
- Требует `gosdk-core` v1.1.0: поднимать вместе с `gosdk-http-core` ≥ v1.2.0, иначе сервис теряет
  AppInfo (имя сервиса) в логах запросов и событиях Sentry. AppInfo читать через
  `context.GetAppInfoFromContext`.
- Логгер экранирует управляющие символы в сообщениях (многострочное сообщение - одна строка с `\n`),
  `logger.Dump` пишет значение одной строкой.

## v1.0.3

Повтор v1.0.2 под новым именем репозитория: `github.com/exgamer/gosdk-kafka-core` (было
`exgamer/github.com-exgamer-gosdk-kafka-core`, путь модуля не совпадал с репозиторием — `go get` не работал).
Код, API и зависимости — как в v1.0.2.

## v1.0.2

Минимальная версия Go поднята до 1.27.3. API и поведение пакета не изменились.

### Изменено
- `go.mod`: `go 1.26.0` → `go 1.27.3`. Причина - уязвимости стандартной библиотеки go1.26.0
  (`govulncheck`), исправлены в новых версиях Go. На go1.27.3 `govulncheck` уязвимостей стандартной библиотеки не находит.
- `gosdk-core` v1.0.28 → v1.0.29 (тоже `go 1.27.3`).

### ⚠️ Изменения поведения
- Потребителю нужен Go ≥ 1.27.3: после `go get` этой версии в его `go.mod` станет `go 1.27.3`.
  С `GOTOOLCHAIN=auto` toolchain скачается сам; с `GOTOOLCHAIN=local` на Go < 1.27.3 или в образе
  без доступа к `proxy.golang.org` сборка упадёт - образ сборки `golang:1.27.3`.

## v1.0.1

`KAFKA_*` из переменных окружения процесса (Docker, k8s, `docker run -e`) читаются
и без `.env`. Типы, поля и сигнатуры не изменились.

### Исправлено
- У полей `config.KafkaConfig` есть `json`-теги с теми же именами, что `mapstructure`
  (`json:"KAFKA_BROKERS"` и т. д.). Без них `InitConfig` из `gosdk-core` находил переменную
  в окружении, но не мог положить её в поле: применялись только ключи, которые есть в `.env`.

### Изменено
- `gosdk-core` поднят с v1.0.26 до v1.0.28: v1.0.27 - `InitConfig` приводит числа и флаги
  из окружения процесса к типу поля, v1.0.28 - `logger.Error` пишет на уровне Error.

### ⚠️ Изменения поведения
- Переменная `KAFKA_*`, которой нет в `.env`, но которая задана в окружении процесса,
  теперь применяется (раньше молча игнорировалась, поле оставалось пустым). Если `.env` нет совсем,
  конфиг целиком берётся из окружения. Для ключей, которые есть в `.env`, окружение было главнее
  и раньше.
- Пустая переменная в окружении (`KAFKA_CLIENT_ID=`) теперь перекрывает значение из `.env`
  пустой строкой (раньше бралось значение из `.env`). Не задавайте пустые `KAFKA_*`
  в окружении, если значение должно прийти из `.env`.
- `json.Marshal(KafkaConfig)` выдаёт ключи `KAFKA_*` вместо имён полей (`Brokers`, `ClientID`…).
  В SDK конфиг в JSON не сериализуется.
- Затронуты все места, где конфиг читается через `InitKafkaConfig`: `app.KafkaKernel`
  (`Init`) и сервисы, которые подключают этот kernel.
- Сообщения `logger.Error` (`gosdk-core` v1.0.28) видны при `LOG_LEVEL` `info` и выше с меткой
  `[ERROR]` (раньше - только при `trace`/`debug` с меткой `[DEBUG]`): остановка consumer
  в `KafkaKernel`, ошибка регистрации обработчика и создания подписчика в `kafkamq.Consumer`.
- Числовые и булевы поля других конфигов сервиса из окружения процесса разбираются по правилам
  `gosdk-core` v1.0.27; неверное значение - ошибка с именем переменной. У `KafkaConfig`
  все поля строковые, для него ничего не меняется.

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
