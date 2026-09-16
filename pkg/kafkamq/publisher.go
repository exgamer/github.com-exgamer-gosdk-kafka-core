package kafkamq

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
	"github.com/google/uuid"
)

type Publisher struct {
	pub    *wmkafka.Publisher
	logger watermill.LoggerAdapter

	mu     sync.Mutex
	closed bool
}

func NewPublisher(kafkaCfg *config.KafkaConfig, cfg ...config.PublisherOption) (*Publisher, error) {
	if kafkaCfg == nil {
		return nil, errors.New("nil kafka config")
	}

	pc := &wmkafka.PublisherConfig{Brokers: kafkaCfg.BrokersList()}

	if kafkaCfg.ClientID != "" {
		saramaCfg := wmkafka.DefaultSaramaSyncPublisherConfig()
		saramaCfg.ClientID = kafkaCfg.ClientID
		pc.OverwriteSaramaConfig = saramaCfg
	}

	for _, opt := range cfg {
		opt(pc)
	}

	log := newWatermillLogger()

	pub, err := wmkafka.NewPublisher(*pc, log)
	if err != nil {
		return nil, fmt.Errorf("error creating publisher: %w", err)
	}

	return &Publisher{
		pub:    pub,
		logger: log,
	}, nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	return p.pub.Close()
}

// Publish - отправка одного сообщения в topic
func (p *Publisher) Publish(topic string, payload any) error {
	msg, err := messageForm(payload)
	if err != nil {
		return err
	}

	if err := p.pub.Publish(topic, msg); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	return nil
}

func (p *Publisher) PublishWithMetaData(topic string, meta map[string]string, payload any) error {
	msg, err := messageFormWithMetaData(meta, payload)
	if err != nil {
		return err
	}

	if err := p.pub.Publish(topic, msg); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	return nil
}

func (p *Publisher) PublishBatch(topic string, payloads []any) error {
	msgs, err := messagesForm(payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	if err := p.pub.Publish(topic, msgs...); err != nil {
		return fmt.Errorf("error publishing messages: %w", err)
	}

	return nil
}

func (p *Publisher) PublishBatchWithMetaData(topic string, meta map[string]string, payloads []any) error {
	msgs, err := messagesFormWithMetaData(meta, payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	if err := p.pub.Publish(topic, msgs...); err != nil {
		return fmt.Errorf("error publishing messages: %w", err)
	}

	return nil
}

// --- message helpers

func messageForm(payload any) (*message.Message, error) {
	val, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}
	return message.NewMessage(uuid.New().String(), val), nil
}

func messagesForm[T any](payloads []T) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(payloads))
	for _, payload := range payloads {
		msg, err := messageForm(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

func messageFormWithMetaData(meta map[string]string, payload any) (*message.Message, error) {
	val, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	msg := message.NewMessage(uuid.New().String(), val)
	for k, v := range meta {
		msg.Metadata.Set(k, v)
	}
	return msg, nil
}

func messagesFormWithMetaData[T any](meta map[string]string, payloads []T) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(payloads))
	for _, payload := range payloads {
		msg, err := messageFormWithMetaData(meta, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}
