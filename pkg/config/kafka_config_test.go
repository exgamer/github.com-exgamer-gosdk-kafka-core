package config

import (
	"reflect"
	"testing"
)

func TestKafkaConfig_BrokersList(t *testing.T) {
	c := KafkaConfig{Brokers: "broker1:9092, broker2:9092,,broker3:9092"}

	want := []string{"broker1:9092", "broker2:9092", "broker3:9092"}
	got := c.BrokersList()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BrokersList() = %v, want %v", got, want)
	}
}

func TestKafkaConfig_BrokersList_Empty(t *testing.T) {
	c := KafkaConfig{}

	got := c.BrokersList()

	if len(got) != 0 {
		t.Fatalf("BrokersList() = %v, want empty", got)
	}
}
