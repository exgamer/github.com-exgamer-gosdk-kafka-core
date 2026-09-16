package config

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type fakeMarshaler struct{}

func (fakeMarshaler) Marshal(_ string, _ *message.Message) (*sarama.ProducerMessage, error) {
	return &sarama.ProducerMessage{}, nil
}

func (fakeMarshaler) Unmarshal(_ *sarama.ConsumerMessage) (*message.Message, error) {
	return nil, nil
}

func TestWithCustomMarshaler(t *testing.T) {
	var cfg kafka.PublisherConfig

	WithCustomMarshaler(fakeMarshaler{})(&cfg)

	if _, ok := cfg.Marshaler.(fakeMarshaler); !ok {
		t.Fatalf("WithCustomMarshaler() did not set config.Marshaler to the provided implementation, got %T", cfg.Marshaler)
	}
}

func TestWithCustomUnmarshaler(t *testing.T) {
	var cfg kafka.SubscriberConfig

	WithCustomUnmarshaler(fakeMarshaler{})(&cfg)

	if _, ok := cfg.Unmarshaler.(fakeMarshaler); !ok {
		t.Fatalf("WithCustomUnmarshaler() did not set config.Unmarshaler to the provided implementation, got %T", cfg.Unmarshaler)
	}
}

func TestWithPublisherBrokers(t *testing.T) {
	var cfg kafka.PublisherConfig

	want := []string{"broker1:9092", "broker2:9092"}
	WithPublisherBrokers(want)(&cfg)

	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != want[0] || cfg.Brokers[1] != want[1] {
		t.Fatalf("WithPublisherBrokers() = %+v, want %+v", cfg.Brokers, want)
	}
}

func TestWithConsumerGroup(t *testing.T) {
	var cfg kafka.SubscriberConfig

	WithConsumerGroup("my-group")(&cfg)

	if cfg.ConsumerGroup != "my-group" {
		t.Fatalf("WithConsumerGroup() = %q, want %q", cfg.ConsumerGroup, "my-group")
	}
}
