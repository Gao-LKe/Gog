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

func TestFromEnvUsesAdministratorAddressForAlerts(t *testing.T) {
	t.Setenv("BOOTSTRAP_ADMIN_EMAIL", "admin@example.test")
	t.Setenv("ALERT_EMAIL", "")
	if cfg := FromEnv(); cfg.AlertEmail != "admin@example.test" {
		t.Fatalf("alert email = %q", cfg.AlertEmail)
	}
}

func TestFromEnvReadsAlertSettings(t *testing.T) {
	t.Setenv("ALERT_EMAIL", "ops@example.test")
	t.Setenv("ALERT_COOLDOWN", "45m")
	t.Setenv("ALERT_CONSECUTIVE_SAMPLES", "3")
	t.Setenv("ALERT_HTTP_MIN_REQUESTS", "50")
	t.Setenv("ALERT_HTTP_ERROR_RATE", "0.1")
	t.Setenv("ALERT_HTTP_P95", "3s")
	cfg := FromEnv()
	if cfg.AlertEmail != "ops@example.test" || cfg.AlertCooldown.String() != "45m0s" || cfg.AlertConsecutiveSamples != 3 || cfg.AlertHTTPMinRequests != 50 || cfg.AlertHTTPErrorRate != .1 || cfg.AlertHTTPP95.String() != "3s" {
		t.Fatalf("unexpected alert config: %+v", cfg)
	}
}
