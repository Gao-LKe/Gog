package platform

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// The platform interfaces keep infrastructure construction outside feature
// modules. Adapters for database/sql, go-redis and Kafka can be supplied here.
type MySQLClient interface {
	PingContext(context.Context) error
}

type RedisClient interface {
	Ping(context.Context) error
}

type KafkaProducer interface {
	Publish(context.Context, string, string, []byte) error
	Ping(context.Context) error
}

type Dependencies struct {
	MySQL MySQLClient
	Redis RedisClient
	Kafka KafkaProducer
	Gorm  *gorm.DB
}

type Clients struct {
	Dependencies
	mysql *sql.DB
	redis *redis.Client
}

func Open(mysqlDSN, redisAddr, kafkaBrokers string) (*Clients, error) {
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	gormDB, err := gorm.Open(mysql.New(mysql.Config{Conn: db}), &gorm.Config{})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	producer := NewKafkaProducer(kafkaBrokers)
	return &Clients{
		Dependencies: Dependencies{MySQL: db, Redis: redisAdapter{rdb}, Kafka: producer, Gorm: gormDB},
		mysql:        db,
		redis:        rdb,
	}, nil
}

func (c *Clients) Close() error {
	if c == nil {
		return nil
	}
	var joined error
	if c.redis != nil {
		joined = errors.Join(joined, c.redis.Close())
	}
	if c.mysql != nil {
		joined = errors.Join(joined, c.mysql.Close())
	}
	return joined
}

type redisAdapter struct{ client *redis.Client }

func (r redisAdapter) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

type kafkaProducer struct{ brokers []string }

func NewKafkaProducer(raw string) KafkaProducer {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return &kafkaProducer{brokers: brokers}
}

func (p *kafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	if len(p.brokers) == 0 {
		return errors.New("kafka broker is not configured")
	}
	w := &kafka.Writer{Addr: kafka.TCP(p.brokers...), Topic: topic, RequiredAcks: kafka.RequireOne}
	defer w.Close()
	return w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value})
}

func (p *kafkaProducer) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return errors.New("kafka broker is not configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}
