package kafkamq

import (
	"bytes"
	"context"
	"fmt"
	stdlog "log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-core/pkg/logger"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

func noopHandler(_ context.Context, _ *message.Message) error { return nil }

// TestRegisterHandler_WarnsWhenConsumerGroupEmpty — без ConsumerGroup (ни из KafkaConfig,
// ни из явного config.WithConsumerGroup) Ack/Nack не коммитят offset в Kafka; это не должно
// проходить незаметно, поэтому RegisterHandler обязан залогировать предупреждение.
func TestRegisterHandler_WarnsWhenConsumerGroupEmpty(t *testing.T) {
	var buf bytes.Buffer
	logger.SetLogger(stdlog.New(&buf, "", 0))
	t.Cleanup(func() { logger.SetLogger(stdlog.New(os.Stdout, "", 0)) })

	c, err := NewConsumer(&config.KafkaConfig{Brokers: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	if err := c.RegisterHandler(context.Background(), noopHandler, "orders"); err != nil {
		t.Fatalf("RegisterHandler() error = %v", err)
	}

	if !strings.Contains(buf.String(), "kafka consumer group is empty") {
		t.Fatalf("expected warning about empty consumer group, got log: %q", buf.String())
	}
}

// TestRegisterHandler_NoWarningWhenConsumerGroupSet — при явно заданной группе (через опцию
// или дефолт из KafkaConfig.ConsumerGroup) предупреждения быть не должно.
func TestRegisterHandler_NoWarningWhenConsumerGroupSet(t *testing.T) {
	var buf bytes.Buffer
	logger.SetLogger(stdlog.New(&buf, "", 0))
	t.Cleanup(func() { logger.SetLogger(stdlog.New(os.Stdout, "", 0)) })

	c, err := NewConsumer(&config.KafkaConfig{Brokers: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	if err := c.RegisterHandler(context.Background(), noopHandler, "orders", config.WithConsumerGroup("orders-service")); err != nil {
		t.Fatalf("RegisterHandler() error = %v", err)
	}

	if strings.Contains(buf.String(), "kafka consumer group is empty") {
		t.Fatalf("did not expect empty consumer group warning, got log: %q", buf.String())
	}
}

// TestRegisterHandler_DefaultConsumerGroupFromKafkaConfig — KafkaConfig.ConsumerGroup должен
// реально использоваться как дефолт, если handler не переопределяет группу явно.
func TestRegisterHandler_DefaultConsumerGroupFromKafkaConfig(t *testing.T) {
	var buf bytes.Buffer
	logger.SetLogger(stdlog.New(&buf, "", 0))
	t.Cleanup(func() { logger.SetLogger(stdlog.New(os.Stdout, "", 0)) })

	c, err := NewConsumer(&config.KafkaConfig{Brokers: "127.0.0.1:0", ConsumerGroup: "default-service"})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	if err := c.RegisterHandler(context.Background(), noopHandler, "orders"); err != nil {
		t.Fatalf("RegisterHandler() error = %v", err)
	}

	if strings.Contains(buf.String(), "kafka consumer group is empty") {
		t.Fatalf("expected KafkaConfig.ConsumerGroup to be used as default, got warning in log: %q", buf.String())
	}
}

// TestConsumer_RegisterHandler_ConcurrentSafe — RegisterHandler (запись в handlers) и
// snapshotHandlers (то, что использует Consume/Close) должны быть безопасны при конкурентном
// доступе — запускать под `go test -race`.
func TestConsumer_RegisterHandler_ConcurrentSafe(t *testing.T) {
	c, err := NewConsumer(&config.KafkaConfig{Brokers: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	const n = 50

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", i)
			if err := c.RegisterHandler(context.Background(), noopHandler, topic, config.WithConsumerGroup("g")); err != nil {
				t.Errorf("RegisterHandler(%s) error = %v", topic, err)
			}
		}(i)
	}

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for i := 0; i < n*2; i++ {
			_ = c.snapshotHandlers()
		}
	}()

	wg.Wait()
	<-readerDone

	if got := len(c.snapshotHandlers()); got != n {
		t.Fatalf("expected %d handlers registered, got %d", n, got)
	}
}
