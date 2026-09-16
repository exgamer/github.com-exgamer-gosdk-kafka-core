package kafkamq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-core/pkg/errorreporter"
	"github.com/exgamer/gosdk-core/pkg/logger"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// Consumer — helper для запуска нескольких подписчиков (handlers).
type Consumer struct {
	kafkaCfg *config.KafkaConfig

	mu       sync.RWMutex
	handlers []config.ConsumeConfig

	logger watermill.LoggerAdapter

	// настройки поведения
	onError       ErrorAction
	onPanic       ErrorAction
	sentryPayload int // max bytes payload to send to sentry extras (0 = unlimited)
}

// ErrorAction определяет, что делать с сообщением при ошибке/панике.
type ErrorAction int

const (
	// ActionAck — подтвердить сообщение (сообщение будет потеряно, ретрая не будет)
	ActionAck ErrorAction = iota
	// ActionNack — отклонить сообщение (redelivery через NackResendSleep, см. config.WithNackResendSleep)
	ActionNack
)

func NewConsumer(kafkaCfg *config.KafkaConfig) (*Consumer, error) {
	if kafkaCfg == nil {
		return nil, errors.New("nil kafka config")
	}

	return &Consumer{
		kafkaCfg:      kafkaCfg,
		logger:        newWatermillLogger(),
		onError:       ActionNack,
		onPanic:       ActionNack,
		sentryPayload: 2048,
	}, nil
}

// WithErrorAction — политика при ошибке handler-а.
func (a *Consumer) WithErrorAction(action ErrorAction) *Consumer {
	a.onError = action

	return a
}

// WithPanicAction — политика при panic в handler-е.
func (a *Consumer) WithPanicAction(action ErrorAction) *Consumer {
	a.onPanic = action

	return a
}

// WithSentryPayloadLimit — лимит payload (bytes) для sentry extras. 0 = без лимита.
func (a *Consumer) WithSentryPayloadLimit(maxBytes int) *Consumer {
	a.sentryPayload = maxBytes

	return a
}

// RegisterMultipleHandler — регистрация набора консьюмеров.
func (a *Consumer) RegisterMultipleHandler(ctx context.Context, handlers []config.HandlerRegister) error {
	for _, h := range handlers {
		if err := a.RegisterHandler(ctx, h.Handler, h.Topic, h.Config...); err != nil {
			logger.Error(ctx, "failed to register kafka handler: "+err.Error())

			return err
		}
	}

	return nil
}

// RegisterHandler — регистрация одного консьюмера на topic.
func (a *Consumer) RegisterHandler(ctx context.Context, handler config.Handler, topic string, cfg ...config.ConsumerOption) error {
	if topic == "" {
		return errors.New("kafka topic is empty")
	}

	sc := &wmkafka.SubscriberConfig{
		Brokers:       a.kafkaCfg.BrokersList(),
		ConsumerGroup: a.kafkaCfg.ConsumerGroup,
	}

	if a.kafkaCfg.ClientID != "" {
		saramaCfg := wmkafka.DefaultSaramaSubscriberConfig()
		saramaCfg.ClientID = a.kafkaCfg.ClientID
		sc.OverwriteSaramaConfig = saramaCfg
	}

	for _, opt := range cfg {
		opt(sc)
	}

	if sc.ConsumerGroup == "" {
		// Без consumer group Kafka не хранит committed offset для этого подписчика:
		// msg.Ack()/Nack() ничего не коммитят (sess == nil в watermill-kafka), и после
		// каждого реконнекта/рестарта сообщения будут вычитаны заново с offset-а,
		// заданного OverwriteSaramaConfig.Consumer.Offsets.Initial. Это осознанный режим
		// для отдельных сценариев (полное вычитывание compacted-топика и т.п.), но чаще
		// всего означает забытый config.WithConsumerGroup/NewConsumerFromOldestConfig.
		logger.Warning(ctx, "kafka consumer group is empty for topic="+topic+": Ack/Nack will not commit any offset, messages will be replayed on every reconnect")
	}

	subscriber, err := wmkafka.NewSubscriber(*sc, a.logger)
	if err != nil {
		logger.Error(ctx, "failed to create kafka subscriber: "+err.Error())

		return err
	}

	a.mu.Lock()
	a.handlers = append(a.handlers, config.ConsumeConfig{
		Consumer: subscriber,
		Handler:  handler,
		Topic:    topic,
	})
	a.mu.Unlock()

	return nil
}

// snapshotHandlers возвращает копию зарегистрированных handlers под RLock — Consume и Close
// не должны читать поле handlers напрямую, чтобы не гонять его конкурентно с RegisterHandler.
func (a *Consumer) snapshotHandlers() []config.ConsumeConfig {
	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make([]config.ConsumeConfig, len(a.handlers))
	copy(out, a.handlers)

	return out
}

// RunConsumers — удобный метод: регистрирует и запускает Consume.
// ВАЖНО: передавайте ctx, чтобы можно было остановить обработку и корректно закрыть подписчиков.
func (a *Consumer) RunConsumers(ctx context.Context, handlers []config.HandlerRegister) error {
	if err := a.RegisterMultipleHandler(ctx, handlers); err != nil {
		return err
	}

	return a.Consume(ctx)
}

// Consume — запускает все зарегистрированные handlers.
// Поведение: при первой ошибке подписки (Subscribe) — отменяем общий контекст и возвращаем ошибку.
// Обработка сообщений: handler ok => Ack; handler err/panic => Ack/Nack по политике.
func (a *Consumer) Consume(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}

	handlers := a.snapshotHandlers()
	if len(handlers) == 0 {
		return errors.New("no handlers registered")
	}

	// общий контекст: если один консьюмер упал при subscribe — останавливаем всех
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(handlers))
	var wg sync.WaitGroup

	for _, consumeCfg := range handlers {
		wg.Add(1)

		go func(cc config.ConsumeConfig) {
			defer wg.Done()

			// Защита от паники на уровне консьюмера (вне обработки конкретного msg)
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("consumer goroutine panic: %v", r)
					a.captureSentry(ctx, err, cc.Handler, nil, true)
					// если такая паника — останавливаем всех
					select {
					case errCh <- err:
					default:
					}
					cancel()
				}
			}()

			handlerName := getHandlerName(cc.Handler)
			logger.Info(ctx, "Try to subscribe handler="+handlerName+" topic="+cc.Topic)

			msgChannel, err := cc.Consumer.Subscribe(ctx, cc.Topic)
			if err != nil {
				wrapped := fmt.Errorf("failed to subscribe (handler=%s, topic=%s): %w", handlerName, cc.Topic, err)
				a.captureSentry(ctx, wrapped, cc.Handler, nil, false)

				select {
				case errCh <- wrapped:
				default:
				}
				cancel()
				return
			}

			logger.Info(ctx, "Successfully subscribed handler="+handlerName+" topic="+cc.Topic)

			for {
				select {
				case <-ctx.Done():
					logger.Info(ctx, "Context cancelled, stopping consumer handler="+handlerName)

					return

				case msg, ok := <-msgChannel:
					if !ok {
						// Канал закрылся: обычно это признак остановки subscriber-а.
						logger.Info(ctx, "Message channel closed handler="+handlerName)

						return
					}

					a.handleMessage(ctx, cc, msg)
				}
			}
		}(consumeCfg)
	}

	// закрываем errCh, когда все горутины завершились
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// ждем первую ошибку или завершение контекста
	var firstErr error
	select {
	case <-ctx.Done():
		// если ctx отменили снаружи — это нормальная остановка
		// но если cancel был из-за ошибки, firstErr может прийти в errCh, поэтому ниже дочитаем
	case firstErr = <-errCh:
		// получили ошибку
	}

	// дочитать, если что-то еще успело упасть — но вернем первую
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (a *Consumer) handleMessage(ctx context.Context, cc config.ConsumeConfig, msg *message.Message) {
	handlerName := getHandlerName(cc.Handler)

	// Защита от паники внутри конкретного сообщения
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic in handler=%s: %v", handlerName, r)
			log.Printf("Panic: %v", err)

			a.captureSentry(ctx, err, cc.Handler, msg, true)
			a.applyAction(msg, a.onPanic)
		}
	}()

	start := time.Now()
	err := cc.Handler(ctx, msg)
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("Error handling message handler=%s elapsed=%s err=%v", handlerName, elapsed, err)
		a.captureSentry(ctx, err, cc.Handler, msg, false)
		a.applyAction(msg, a.onError)

		return
	}

	// успех
	msg.Ack()
}

func (a *Consumer) applyAction(msg *message.Message, action ErrorAction) {
	switch action {
	case ActionAck:
		msg.Ack()
	default:
		// ActionNack (по умолчанию)
		msg.Nack()
	}
}

// captureSentry отправляет ошибку в error-трекер через errorreporter.Capture.
// Consumer ничего не знает про Sentry - реальную отправку делает адаптер,
// зарегистрированный через errorreporter.SetReporter (см. gosdk-sentry-core).
// Без него вызов безопасен и просто ничего не отправляет.
func (a *Consumer) captureSentry(ctx context.Context, err error, handler config.Handler, msg *message.Message, isPanic bool) {
	handlerName := getHandlerName(handler)

	consumerCtx := map[string]any{
		"handler": handlerName,
		"panic":   isPanic,
	}

	if msg != nil {
		consumerCtx["message_uuid"] = msg.UUID

		if a.sentryPayload != 0 && len(msg.Payload) > a.sentryPayload {
			consumerCtx["payload"] = string(msg.Payload[:a.sentryPayload])
			consumerCtx["payload_truncated"] = true
			consumerCtx["payload_size"] = len(msg.Payload)
		} else {
			consumerCtx["payload"] = string(msg.Payload)
			consumerCtx["payload_truncated"] = false
		}

		// метаданные иногда полезны, но могут быть большими — оставим как есть
		if msg.Metadata != nil {
			consumerCtx["metadata"] = msg.Metadata
		}
	}

	// Level: error для всех случаев - как и раньше (в исходном коде
	// scope.SetLevel не звался, sentry-go по умолчанию шлёт LevelError).
	errorreporter.Capture(ctx, err, errorreporter.Options{
		Level: errorreporter.LevelError,
		// handler тегом (не только в Extra) - чтобы фильтровать/группировать
		// issues в Sentry по конкретному консьюмеру, как endpoint для HTTP.
		Tags: map[string]string{
			"component": "kafka_consumer",
			"handler":   handlerName,
		},
		Extra: map[string]any{"consumer": consumerCtx},
	})
}

// Close закрывает всех зарегистрированных subscriber-ов. В отличие от Rabbit
// (где subscriber-ы делят общий connection, закрываемый kernel-ом), в Kafka
// каждый subscriber держит собственный sarama-клиент и должен закрываться явно.
func (a *Consumer) Close() error {
	var firstErr error

	for _, cc := range a.snapshotHandlers() {
		if cc.Consumer == nil {
			continue
		}

		if err := cc.Consumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func getHandlerName(handler config.Handler) string {
	handlerName := reflect.TypeOf(handler).String()

	if reflect.TypeOf(handler).Kind() == reflect.Func {
		pc := reflect.ValueOf(handler).Pointer()
		if fn := runtime.FuncForPC(pc); fn != nil {
			handlerName = fn.Name()
		}
	}

	return handlerName
}
