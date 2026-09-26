package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains the small set of runtime settings needed by the skeleton.
// Concrete MySQL, Redis and Kafka clients are intentionally injected by the
// composition root rather than constructed inside feature packages.
type Config struct {
	Environment              string
	HTTPAddr                 string
	MetricsAddr              string
	PrometheusURL            string
	AuthSecret               string
	AuthIssuer               string
	AuthAudience             string
	AccessTokenTTL           time.Duration
	RefreshTokenTTL          time.Duration
	EmailCodeTTL             time.Duration
	EmailMode                string
	SMTPHost                 string
	SMTPPort                 string
	SMTPUsername             string
	SMTPPassword             string
	SMTPFrom                 string
	BootstrapAdminEmail      string
	SnowflakeNodeID          int64
	RateLimitPerIP           int
	RealtimeMaxConnections   int
	RealtimeFallbackCooldown time.Duration
	MySQLDSN                 string
	RedisAddr                string
	KafkaBroker              string
	OrderConsumerWorkers     int
	OrderTopicPartitions     int
}

func FromEnv() Config {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = "change-me-in-production"
	}
	return Config{
		Environment:              valueOrDefault("APP_ENV", "development"),
		HTTPAddr:                 addr,
		MetricsAddr:              valueOrDefault("METRICS_ADDR", ":9091"),
		PrometheusURL:            valueOrDefault("PROMETHEUS_URL", "http://localhost:9090"),
		AuthSecret:               secret,
		AuthIssuer:               valueOrDefault("AUTH_ISSUER", "goblog"),
		AuthAudience:             valueOrDefault("AUTH_AUDIENCE", "goblog-api"),
		AccessTokenTTL:           durationOrDefault("ACCESS_TOKEN_TTL", 15*time.Minute),
		RefreshTokenTTL:          durationOrDefault("REFRESH_TOKEN_TTL", 30*24*time.Hour),
		EmailCodeTTL:             durationOrDefault("EMAIL_CODE_TTL", 5*time.Minute),
		EmailMode:                valueOrDefault("EMAIL_MODE", "log"),
		SMTPHost:                 strings.TrimSpace(os.Getenv("SMTP_HOST")),
		SMTPPort:                 valueOrDefault("SMTP_PORT", "465"),
		SMTPUsername:             strings.TrimSpace(os.Getenv("SMTP_USERNAME")),
		SMTPPassword:             strings.TrimSpace(os.Getenv("SMTP_PASSWORD")),
		SMTPFrom:                 strings.TrimSpace(os.Getenv("SMTP_FROM")),
		BootstrapAdminEmail:      strings.TrimSpace(os.Getenv("BOOTSTRAP_ADMIN_EMAIL")),
		SnowflakeNodeID:          snowflakeNodeIDFromEnv(),
		RateLimitPerIP:           positiveIntOrDefault("RATE_LIMIT_PER_IP", 120),
		RealtimeMaxConnections:   positiveIntOrDefault("REALTIME_MAX_CONNECTIONS", 1000),
		RealtimeFallbackCooldown: durationOrDefault("REALTIME_FALLBACK_COOLDOWN", time.Minute),
		MySQLDSN:                 valueOrDefault("MYSQL_DSN", "shop:shop_dev_password@tcp(localhost:3306)/shop_core?parseTime=true"),
		RedisAddr:                valueOrDefault("REDIS_ADDR", "localhost:6379"),
		KafkaBroker:              strings.TrimSpace(valueOrDefault("KAFKA_BROKERS", "localhost:9092")),
		OrderConsumerWorkers:     positiveIntOrDefault("ORDER_CONSUMER_WORKERS", 8),
		OrderTopicPartitions:     positiveIntOrDefault("ORDER_TOPIC_PARTITIONS", 8),
	}
}

func (c Config) Validate() error {
	if c.MetricsAddr == "" || c.PrometheusURL == "" {
		return errors.New("METRICS_ADDR and PROMETHEUS_URL must be configured")
	}
	if c.EmailMode == "smtp" && (c.SMTPHost == "" || c.SMTPUsername == "" || c.SMTPPassword == "" || c.SMTPFrom == "") {
		return errors.New("SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD and SMTP_FROM are required when EMAIL_MODE=smtp")
	}
	if c.EmailMode != "log" && c.EmailMode != "smtp" {
		return errors.New("EMAIL_MODE must be log or smtp")
	}
	if c.SnowflakeNodeID < 0 || c.SnowflakeNodeID > 1023 {
		return errors.New("SNOWFLAKE_NODE_ID must be between 0 and 1023")
	}
	if c.RateLimitPerIP <= 0 {
		return errors.New("rate limit per IP must be positive")
	}
	if c.RealtimeMaxConnections <= 0 || c.RealtimeFallbackCooldown <= 0 {
		return errors.New("realtime connection limit and fallback cooldown must be positive")
	}
	if c.OrderConsumerWorkers <= 0 || c.OrderTopicPartitions <= 0 {
		return errors.New("order consumer workers and topic partitions must be positive")
	}
	if c.Environment != "production" {
		return nil
	}
	if c.AuthSecret == "change-me-in-production" || len(c.AuthSecret) < 32 {
		return errors.New("JWT_SECRET must be at least 32 characters in production")
	}
	if c.AuthIssuer == "" || c.AuthAudience == "" {
		return errors.New("AUTH_ISSUER and AUTH_AUDIENCE are required in production")
	}
	return nil
}

func snowflakeNodeIDFromEnv() int64 {
	value := strings.TrimSpace(os.Getenv("SNOWFLAKE_NODE_ID"))
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return -1 // Validate turns an invalid value into a startup error.
	}
	return parsed
}

func valueOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func durationOrDefault(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}

func positiveIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	parsed, err := strconv.Atoi(value)
	if value == "" || err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
