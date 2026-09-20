package config

import (
	"os"
	"strings"
)

// Config contains the small set of runtime settings needed by the skeleton.
// Concrete MySQL, Redis and Kafka clients are intentionally injected by the
// composition root rather than constructed inside feature packages.
type Config struct {
	HTTPAddr    string
	AuthSecret  string
	MySQLDSN    string
	RedisAddr   string
	KafkaBroker string
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
		HTTPAddr:    addr,
		AuthSecret:  secret,
		MySQLDSN:    valueOrDefault("MYSQL_DSN", "shop:shop_dev_password@tcp(localhost:3306)/shop_core?parseTime=true"),
		RedisAddr:   valueOrDefault("REDIS_ADDR", "localhost:6379"),
		KafkaBroker: strings.TrimSpace(valueOrDefault("KAFKA_BROKERS", "localhost:9092")),
	}
}

func valueOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
