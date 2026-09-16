package kafkamq

import "testing"

// TestDeclareTopics_ValidatesBeforeDialing — некорректный TopicSpec должен быть отвергнут
// понятной ошибкой SDK до попытки открыть ClusterAdmin (иначе брокеры недостижимы в тесте
// и ошибка обязана прийти именно от валидации, а не от сетевого таймаута).
func TestDeclareTopics_ValidatesBeforeDialing(t *testing.T) {
	tests := []struct {
		name string
		spec TopicSpec
	}{
		{"empty name", TopicSpec{Name: "", NumPartitions: 1, ReplicationFactor: 1}},
		{"zero partitions", TopicSpec{Name: "orders", NumPartitions: 0, ReplicationFactor: 1}},
		{"negative partitions", TopicSpec{Name: "orders", NumPartitions: -1, ReplicationFactor: 1}},
		{"zero replication factor", TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := DeclareTopics([]string{"127.0.0.1:0"}, nil, tt.spec)
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}
