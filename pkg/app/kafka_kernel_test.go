package app

import (
	"testing"

	coreApp "github.com/exgamer/gosdk-core/pkg/app"
	coreDi "github.com/exgamer/gosdk-core/pkg/di"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// TestKafkaKernel_Init_RegistriesGatedByEnableFlags — без EnableConsumer()/EnablePublisher()
// ни один из реестров не должен попадать в DI: иначе флаги ничего не переключают.
func TestKafkaKernel_Init_RegistriesGatedByEnableFlags(t *testing.T) {
	a := &coreApp.App{Container: coreDi.NewContainer()}

	k := NewKafkaKernel()

	if err := k.Init(a); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if _, err := coreDi.Resolve[*PublisherRegistry](a.Container); err == nil {
		t.Fatal("expected PublisherRegistry to NOT be registered without EnablePublisher()")
	}

	if _, err := coreDi.Resolve[*ConsumersRegistry](a.Container); err == nil {
		t.Fatal("expected ConsumersRegistry to NOT be registered without EnableConsumer()")
	}

	// Конфиг должен быть доступен всегда — нужен для kafkamq.DeclareTopics без consumer/publisher.
	if _, err := coreDi.Resolve[*config.KafkaConfig](a.Container); err != nil {
		t.Fatalf("expected KafkaConfig to always be registered: %v", err)
	}
}

// TestKafkaKernel_Init_EnabledRegistriesAreRegistered — с обоими флагами оба реестра
// должны быть резолвимы из DI.
func TestKafkaKernel_Init_EnabledRegistriesAreRegistered(t *testing.T) {
	a := &coreApp.App{Container: coreDi.NewContainer()}

	k := NewKafkaKernel().EnableConsumer().EnablePublisher()

	if err := k.Init(a); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if _, err := coreDi.Resolve[*PublisherRegistry](a.Container); err != nil {
		t.Fatalf("expected PublisherRegistry to be registered: %v", err)
	}

	if _, err := coreDi.Resolve[*ConsumersRegistry](a.Container); err != nil {
		t.Fatalf("expected ConsumersRegistry to be registered: %v", err)
	}
}

// TestKafkaKernel_Init_OnlyConsumerEnabled — включение одного флага не должно тянуть за
// собой реестр другого.
func TestKafkaKernel_Init_OnlyConsumerEnabled(t *testing.T) {
	a := &coreApp.App{Container: coreDi.NewContainer()}

	k := NewKafkaKernel().EnableConsumer()

	if err := k.Init(a); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if _, err := coreDi.Resolve[*ConsumersRegistry](a.Container); err != nil {
		t.Fatalf("expected ConsumersRegistry to be registered: %v", err)
	}

	if _, err := coreDi.Resolve[*PublisherRegistry](a.Container); err == nil {
		t.Fatal("expected PublisherRegistry to NOT be registered without EnablePublisher()")
	}
}
