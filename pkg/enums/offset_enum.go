package enums

// OffsetOldest / OffsetNewest - начальный offset для консьюмера без закоммиченного
// прогресса (аналог sarama.OffsetOldest/OffsetNewest, вынесен как отдельная константа,
// чтобы вызывающий код не зависел от sarama напрямую).
const (
	OffsetOldest int64 = -2
	OffsetNewest int64 = -1
)
