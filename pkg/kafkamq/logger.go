package kafkamq

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/exgamer/gosdk-core/pkg/logger"
)

// newWatermillLogger строит watermill-логгер с уровнем debug/trace из gosdk-core logger.
// Kafka (в отличие от Rabbit) не имеет общего переиспользуемого connection-объекта —
// publisher и subscriber поднимают собственные sarama-клиенты, поэтому вместо
// NewAmqpConnection здесь достаточно общего конструктора логгера.
func newWatermillLogger() watermill.LoggerAdapter {
	debug := logger.IsDebugLevel()
	trace := logger.IsTraceLevel()

	return watermill.NewStdLogger(debug, trace)
}
