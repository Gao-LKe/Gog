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
