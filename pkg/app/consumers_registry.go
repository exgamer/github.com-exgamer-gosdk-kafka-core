package app

import (
	"sync"

	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

type ConsumersRegistry struct {
	mu       sync.RWMutex
	handlers []config.HandlerRegister
}

func NewConsumersRegistry() *ConsumersRegistry {
	return &ConsumersRegistry{handlers: make([]config.HandlerRegister, 0)}
}

func (r *ConsumersRegistry) RegisterHandler(h config.HandlerRegister) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, h)
}

func (r *ConsumersRegistry) RegisterMultipleHandler(list []config.HandlerRegister) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, list...)
}

func (r *ConsumersRegistry) List() []config.HandlerRegister {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]config.HandlerRegister, len(r.handlers))
	copy(out, r.handlers)
	return out
}
