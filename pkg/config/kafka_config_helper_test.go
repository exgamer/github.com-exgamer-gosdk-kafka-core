package config

import (
	"os"
	"path/filepath"
	"testing"

	coreconfig "github.com/exgamer/gosdk-core/pkg/config"
)

var kafkaEnvKeys = []string{
	"KAFKA_BROKERS",
	"KAFKA_CONSUMER_GROUP",
	"KAFKA_CLIENT_ID",
}

func TestInitKafkaConfig_OSEnvWithoutEnvFile(t *testing.T) {
	clearKafkaEnv(t)
	loadEnvFile(t, "")
	t.Setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "orders")
	t.Setenv("KAFKA_CLIENT_ID", "orders-service")

	cfg, err := InitKafkaConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	want := KafkaConfig{
		Brokers:       "kafka1:9092,kafka2:9092",
		ConsumerGroup: "orders",
		ClientID:      "orders-service",
	}
	if *cfg != want {
		t.Errorf("got %+v, want %+v", *cfg, want)
	}
}

func TestInitKafkaConfig_OSEnvOverridesEnvFile(t *testing.T) {
	clearKafkaEnv(t)
	loadEnvFile(t, "KAFKA_BROKERS=file:9092\nKAFKA_CONSUMER_GROUP=file_group\n")
	t.Setenv("KAFKA_BROKERS", "env:9092")
	t.Setenv("KAFKA_CLIENT_ID", "env_client")

	cfg, err := InitKafkaConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	if cfg.Brokers != "env:9092" {
		t.Errorf("Brokers = %q, want env:9092", cfg.Brokers)
	}
	if cfg.ClientID != "env_client" {
		t.Errorf("ClientID = %q, want env_client from env", cfg.ClientID)
	}
	if cfg.ConsumerGroup != "file_group" {
		t.Errorf("ConsumerGroup = %q, want file_group from .env", cfg.ConsumerGroup)
	}
}

func TestInitKafkaConfig_EmptyOSEnvOverridesEnvFile(t *testing.T) {
	clearKafkaEnv(t)
	loadEnvFile(t, "KAFKA_CONSUMER_GROUP=file_group\nKAFKA_CLIENT_ID=file_client\n")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	t.Setenv("KAFKA_CLIENT_ID", "")

	cfg, err := InitKafkaConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	if cfg.ConsumerGroup != "" || cfg.ClientID != "" {
		t.Errorf("got ConsumerGroup=%q ClientID=%q, want empty", cfg.ConsumerGroup, cfg.ClientID)
	}
}

func TestInitKafkaConfig_EnvFileOnly(t *testing.T) {
	clearKafkaEnv(t)
	loadEnvFile(t, "KAFKA_BROKERS=file:9092\n"+
		"KAFKA_CONSUMER_GROUP=file_group\n"+
		"KAFKA_CLIENT_ID=file_client\n")

	cfg, err := InitKafkaConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	want := KafkaConfig{
		Brokers:       "file:9092",
		ConsumerGroup: "file_group",
		ClientID:      "file_client",
	}
	if *cfg != want {
		t.Errorf("got %+v, want %+v", *cfg, want)
	}
}

// clearKafkaEnv Убирает KAFKA_* из окружения процесса на время теста.
func clearKafkaEnv(t *testing.T) {
	t.Helper()

	for _, key := range kafkaEnvKeys {
		t.Setenv(key, "")

		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

// loadEnvFile Загружает .env с содержимым content; пустой content — файла нет, только окружение.
func loadEnvFile(t *testing.T, content string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), ".env")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write env file: %v", err)
		}
	}

	if _, err := coreconfig.LoadEnv(path); err != nil {
		t.Fatalf("load env: %v", err)
	}
}
