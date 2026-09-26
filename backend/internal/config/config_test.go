package config

import "testing"

func TestValidateRejectsInvalidSnowflakeNodeID(t *testing.T) {
	for _, nodeID := range []int64{-1, 1024} {
		cfg := Config{EmailMode: "log", SnowflakeNodeID: nodeID}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("node ID %d was accepted", nodeID)
		}
	}
}

func TestFromEnvReadsRateLimitPerIP(t *testing.T) {
	t.Setenv("RATE_LIMIT_PER_IP", "3600")
	if cfg := FromEnv(); cfg.RateLimitPerIP != 3600 {
		t.Fatalf("rate limit = %d, want 3600", cfg.RateLimitPerIP)
	}
}

func TestFromEnvReadsOrderWorkerSettings(t *testing.T) {
	t.Setenv("ORDER_CONSUMER_WORKERS", "6")
	t.Setenv("ORDER_TOPIC_PARTITIONS", "12")
	cfg := FromEnv()
	if cfg.OrderConsumerWorkers != 6 || cfg.OrderTopicPartitions != 12 {
		t.Fatalf("order workers=%d partitions=%d, want 6 and 12", cfg.OrderConsumerWorkers, cfg.OrderTopicPartitions)
	}
}
