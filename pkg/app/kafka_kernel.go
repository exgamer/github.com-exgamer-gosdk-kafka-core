package app

import (
	"context"

	"github.com/exgamer/gosdk-core/pkg/app"
	"github.com/exgamer/gosdk-core/pkg/di"
	"github.com/exgamer/gosdk-core/pkg/logger"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
	"github.com/exgamer/gosdk-kafka-core/pkg/kafkamq"
)

const KafkaKernelName = "kafka"

func NewKafkaKernel() *KafkaKernel {
	return &KafkaKernel{}
}

type KafkaKernel struct {
	config   *config.KafkaConfig
	consumer *kafkamq.Consumer

	consumersRegistry  *ConsumersRegistry
	publishersRegistry *PublisherRegistry

	ctx    context.Context
	cancel context.CancelFunc

	enableConsumer  bool
	enablePublisher bool
}

func (k *KafkaKernel) EnableConsumer() *KafkaKernel {
	k.enableConsumer = true

	return k
}

func (k *KafkaKernel) EnablePublisher() *KafkaKernel {
	k.enablePublisher = true

	return k
}

func (k *KafkaKernel) Name() string {
	return KafkaKernelName
}

func (k *KafkaKernel) Init(a *app.App) error {
	kafkaConfig, err := config.InitKafkaConfig()

	if err != nil {
		return err
	}

	k.config = kafkaConfig

	// Конфиг в DI всегда (чтобы модули могли декларировать топики без необходимости
	// поднимать consumer или publisher, см. kafkamq.DeclareTopics) — не зависит от
	// EnableConsumer/EnablePublisher, в отличие от реестров ниже.
	di.Register(a.Container, k.config)

	// Реестр в DI (чтобы модули могли добавлять publishers) — только если явно включено:
	// EnablePublisher() должен быть реальным переключателем, а не косметикой.
	if k.enablePublisher {
		pubReg := NewPublisherRegistry(k.config)
		di.Register(a.Container, pubReg)
		k.publishersRegistry = pubReg
	}

	// Реестр в DI (чтобы модули могли добавлять handlers) — аналогично, только при EnableConsumer().
	if k.enableConsumer {
		reg := NewConsumersRegistry()
		di.Register(a.Container, reg)
		k.consumersRegistry = reg
	}

	return nil
}

func (k *KafkaKernel) Start(a *app.App) error {
	// Контекст на весь runtime
	k.ctx, k.cancel = context.WithCancel(a.GetContext())

	if k.enableConsumer {
		consumer, err := kafkamq.NewConsumer(k.config)

		if err != nil {
			return err
		}

		di.Register(a.Container, consumer)

		k.consumer = consumer

		handlers := k.consumersRegistry.List()

		// Регистрируем консьюмеры
		if err = k.consumer.RegisterMultipleHandler(k.ctx, handlers); err != nil {
			return err
		}

		go func() {
			if err := k.consumer.Consume(k.ctx); err != nil {
				logger.Error(k.ctx, "kafka consumer stopped: "+err.Error())
				k.cancel()
			}
		}()
	}

	return nil
}

func (k *KafkaKernel) Stop(ctx context.Context) error {
	if k.cancel != nil {
		k.cancel()
	}

	if k.publishersRegistry != nil {
		_ = k.publishersRegistry.Close()
	}

	if k.consumer == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- k.consumer.Close() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
