package app

import (
	"fmt"
	"sync"

	"github.com/exgamer/gosdk-kafka-core/pkg/config"
	"github.com/exgamer/gosdk-kafka-core/pkg/kafkamq"
)

func NewPublisherRegistry(cfg *config.KafkaConfig) *PublisherRegistry {
	return &PublisherRegistry{
		publishers:  make(map[string]*kafkamq.Publisher),
		definitions: make(map[string]config.PublisherDefinition),
		config:      cfg,
	}
}

type PublisherRegistry struct {
	mu          sync.RWMutex
	publishers  map[string]*kafkamq.Publisher
	definitions map[string]config.PublisherDefinition
	config      *config.KafkaConfig
}

func (r *PublisherRegistry) RegisterMultiple(list []config.PublisherDefinition) error {
	for _, def := range list {
		if err := r.Register(def); err != nil {
			return err
		}
	}

	return nil
}

// Register — вызывается разработчиком в модуле (без конфига брокеров!)
func (r *PublisherRegistry) Register(def config.PublisherDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if def.Name == "" {
		return fmt.Errorf("publisher name is empty")
	}

	if _, exists := r.definitions[def.Name]; exists {
		return fmt.Errorf("publisher %q already registered", def.Name)
	}

	r.definitions[def.Name] = def

	return nil
}

func (r *PublisherRegistry) Get(name string) (*kafkamq.Publisher, error) {
	if name == "" {
		return nil, fmt.Errorf("publisher name is empty")
	}

	// Сначала пробуем быстро под RLock
	r.mu.RLock()
	pub, ok := r.publishers[name]
	r.mu.RUnlock()

	if ok {
		return pub, nil
	}

	// Если нет — создаём под Lock
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check (могли создать между RUnlock и Lock)
	if pub, ok = r.publishers[name]; ok {
		return pub, nil
	}

	def, exists := r.definitions[name]
	if !exists {
		return nil, fmt.Errorf("publisher %q is not registered", name)
	}

	if r.config == nil {
		return nil, fmt.Errorf("kafka config is nil, cannot create publisher %q", name)
	}

	newPub, err := kafkamq.NewPublisher(r.config, def.Config...)
	if err != nil {
		return nil, fmt.Errorf("init publisher %q: %w", name, err)
	}

	r.publishers[name] = newPub

	return newPub, nil
}

func (r *PublisherRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error

	for name, pub := range r.publishers {
		if pub == nil {
			continue
		}

		if err := pub.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close publisher %q: %w", name, err)
		}
	}

	// очищаем, чтобы registry можно было безопасно переинициализировать
	r.publishers = make(map[string]*kafkamq.Publisher)

	return firstErr
}
