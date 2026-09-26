package platform

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
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
	Set(context.Context, string, any, time.Duration) error
	Get(context.Context, string) (string, error)
	Has(context.Context, string) (bool, error)
	Del(context.Context, ...string) error
	DeleteIfEqual(context.Context, string, string) (bool, error)
	ConsumeIfEqual(context.Context, string, string) (bool, error)
	SetIfNotExists(context.Context, string, string, time.Duration) (bool, error)
	DecrementIfExists(context.Context, string, uint64) (bool, error)
	Increment(context.Context, string, time.Duration) (int64, error)
}

// RedisSessionRecord is the stable field set stored for an online session.
// The adapter stores these as a Redis hash so refresh-token hashes are never
// mixed into an opaque string value.
type RedisSessionRecord struct {
	UserID      uint64
	DeviceType  string
	Role        string
	Permissions []string
	RefreshHash string
	CreatedAt   int64
	ExpiresAt   int64
}

type RefreshRotationResult uint8

const (
	RefreshRotationRejected RefreshRotationResult = iota
	RefreshRotationSucceeded
	RefreshRotationReplay
)

type RedisSessionKeys struct {
	SlotKey          string
	SessionKeyPrefix string
	SessionKey       string
	RefreshIndexKey  string
	SID              string
}

type RedisRefreshKeys struct {
	SessionKey      string
	ReplayKey       string
	NewRefreshIndex string
	SID             string
}

type RedisAtomicClient interface {
	// ReplaceSession atomically replaces a device slot and its session. It
	// returns the sid that was in the slot before the replacement.
	ReplaceSession(context.Context, RedisSessionKeys, RedisSessionRecord, time.Duration) (string, error)
	GetSession(context.Context, string) (RedisSessionRecord, bool, error)
	// DeleteSessionIfCurrent removes a session and slot only when the slot
	// still points to sid.
	DeleteSessionIfCurrent(context.Context, RedisSessionKeys) (bool, error)
	// RotateRefreshHash compares and replaces a session hash and records the
	// old hash for a short replay window. Replay returns the sid stored in that
	// marker.
	RotateRefreshHash(context.Context, RedisRefreshKeys, string, string, time.Duration) (RefreshRotationResult, string, error)
	IncrementWithTTL(context.Context, string, time.Duration) (int64, error)
}

type KafkaProducer interface {
	Publish(context.Context, string, string, []byte) error
	Ping(context.Context) error
}

type Dependencies struct {
	MySQL       MySQLClient
	Redis       RedisClient
	RedisAtomic RedisAtomicClient
	Kafka       KafkaProducer
	Gorm        *gorm.DB
}

type Clients struct {
	Dependencies
	mysql *sql.DB
	redis *redis.Client
	kafka *kafkaProducer
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
	producer := newKafkaProducer(kafkaBrokers)
	return &Clients{
		Dependencies: Dependencies{MySQL: db, Redis: redisAdapter{rdb}, RedisAtomic: redisAdapter{rdb}, Kafka: producer, Gorm: gormDB},
		mysql:        db,
		redis:        rdb,
		kafka:        producer,
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
	if c.kafka != nil {
		joined = errors.Join(joined, c.kafka.Close())
	}
	return joined
}

type redisAdapter struct{ client *redis.Client }

const (
	sessionUserField    = "user_id"
	sessionDeviceField  = "device_type"
	sessionRoleField    = "role"
	sessionPermField    = "permissions"
	sessionHashField    = "refresh_hash"
	sessionCreatedField = "created_at"
	sessionExpiryField  = "expires_at"
)

var replaceSessionScript = redis.NewScript(`
local old = redis.call('GET', KEYS[1])
if old ~= false and ARGV[11] ~= '' then
	redis.call('DEL', ARGV[11] .. old)
end
redis.call('HSET', KEYS[2], 'user_id', ARGV[2], 'device_type', ARGV[3], 'role', ARGV[4], 'permissions', ARGV[5], 'refresh_hash', ARGV[6], 'created_at', ARGV[7], 'expires_at', ARGV[8])
redis.call('PEXPIRE', KEYS[2], ARGV[9])
redis.call('SET', KEYS[1], ARGV[10], 'PX', ARGV[9])
redis.call('SET', KEYS[3], ARGV[10], 'PX', ARGV[9])
if old == false then return '' end
return old
`)

var deleteSessionIfCurrentScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1], KEYS[2])
return 1
`)

var rotateRefreshHashScript = redis.NewScript(`
local actual = redis.call('HGET', KEYS[1], 'refresh_hash')
if actual == ARGV[1] then
  redis.call('HSET', KEYS[1], 'refresh_hash', ARGV[2])
  redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[4])
	redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[5])
  return {1, ''}
end
local replaySid = redis.call('GET', KEYS[2])
if replaySid ~= false then return {2, replaySid} end
return {0, ''}
`)

var incrementWithTTLScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n
`)

func ttlMillis(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("redis ttl must be positive")
	}
	ms := ttl.Milliseconds()
	if ms <= 0 {
		return 0, errors.New("redis ttl is below one millisecond")
	}
	return ms, nil
}

func (r redisAdapter) ReplaceSession(ctx context.Context, keys RedisSessionKeys, record RedisSessionRecord, ttl time.Duration) (string, error) {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return "", err
	}
	if keys.SlotKey == "" || keys.SessionKeyPrefix == "" || keys.SessionKey == "" || keys.RefreshIndexKey == "" || keys.SID == "" || keys.SessionKey != keys.SessionKeyPrefix+keys.SID || record.UserID == 0 || record.DeviceType == "" || record.Role == "" || record.RefreshHash == "" || record.CreatedAt <= 0 || record.ExpiresAt <= 0 {
		return "", errors.New("redis session replacement requires non-empty keys and fields")
	}
	redisKeys := []string{keys.SlotKey, keys.SessionKey, keys.RefreshIndexKey}
	args := []any{"", strconv.FormatUint(record.UserID, 10), record.DeviceType, record.Role, strings.Join(record.Permissions, ","), record.RefreshHash, record.CreatedAt, record.ExpiresAt, ms, keys.SID, keys.SessionKeyPrefix}
	value, err := replaceSessionScript.Run(ctx, r.client, redisKeys, args...).Text()
	return value, err
}

func (r redisAdapter) GetSession(ctx context.Context, key string) (RedisSessionRecord, bool, error) {
	if key == "" {
		return RedisSessionRecord{}, false, errors.New("redis session key must not be empty")
	}
	values, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return RedisSessionRecord{}, false, err
	}
	if len(values) == 0 {
		return RedisSessionRecord{}, false, nil
	}
	userID, err := strconv.ParseUint(values[sessionUserField], 10, 64)
	if err != nil {
		return RedisSessionRecord{}, false, errors.New("redis session contains invalid user id")
	}
	createdAt, err := strconv.ParseInt(values[sessionCreatedField], 10, 64)
	if err != nil {
		return RedisSessionRecord{}, false, errors.New("redis session contains invalid creation time")
	}
	expiresAt, err := strconv.ParseInt(values[sessionExpiryField], 10, 64)
	if err != nil {
		return RedisSessionRecord{}, false, errors.New("redis session contains invalid expiry")
	}
	permissions := []string{}
	if raw := values[sessionPermField]; raw != "" {
		permissions = strings.Split(raw, ",")
	}
	record := RedisSessionRecord{UserID: userID, DeviceType: values[sessionDeviceField], Role: values[sessionRoleField], Permissions: permissions, RefreshHash: values[sessionHashField], CreatedAt: createdAt, ExpiresAt: expiresAt}
	if record.DeviceType == "" || record.Role == "" || record.RefreshHash == "" || record.CreatedAt <= 0 || record.ExpiresAt <= 0 {
		return RedisSessionRecord{}, false, errors.New("redis session contains incomplete fields")
	}
	return record, true, nil
}

func (r redisAdapter) DeleteSessionIfCurrent(ctx context.Context, keys RedisSessionKeys) (bool, error) {
	if keys.SlotKey == "" || keys.SessionKey == "" || keys.SID == "" {
		return false, errors.New("redis session deletion requires non-empty keys")
	}
	n, err := deleteSessionIfCurrentScript.Run(ctx, r.client, []string{keys.SlotKey, keys.SessionKey}, keys.SID).Int()
	return n == 1, err
}

func (r redisAdapter) RotateRefreshHash(ctx context.Context, keys RedisRefreshKeys, currentHash, newHash string, replayTTL time.Duration) (RefreshRotationResult, string, error) {
	ms, err := ttlMillis(replayTTL)
	if err != nil {
		return RefreshRotationRejected, "", err
	}
	if keys.SessionKey == "" || keys.ReplayKey == "" || keys.NewRefreshIndex == "" || keys.SID == "" || currentHash == "" || newHash == "" {
		return RefreshRotationRejected, "", errors.New("redis refresh rotation requires non-empty fields")
	}
	values, err := rotateRefreshHashScript.Run(ctx, r.client, []string{keys.SessionKey, keys.ReplayKey, keys.NewRefreshIndex}, currentHash, newHash, keys.SID, ms, ms).Slice()
	if err != nil {
		return RefreshRotationRejected, "", err
	}
	if len(values) != 2 {
		return RefreshRotationRejected, "", errors.New("redis refresh rotation returned malformed result")
	}
	status, ok := values[0].(int64)
	if !ok {
		return RefreshRotationRejected, "", errors.New("redis refresh rotation returned invalid status")
	}
	replaySID, _ := values[1].(string)
	switch status {
	case 1:
		return RefreshRotationSucceeded, "", nil
	case 2:
		return RefreshRotationReplay, replaySID, nil
	default:
		return RefreshRotationRejected, "", nil
	}
}

func (r redisAdapter) IncrementWithTTL(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return 0, err
	}
	if key == "" {
		return 0, errors.New("redis counter key must not be empty")
	}
	return incrementWithTTLScript.Run(ctx, r.client, []string{key}, ms).Int64()
}

func (r redisAdapter) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r redisAdapter) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

func (r redisAdapter) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

func (r redisAdapter) Has(ctx context.Context, key string) (bool, error) {
	count, err := r.client.Exists(ctx, key).Result()
	return count > 0, err
}

func (r redisAdapter) Del(ctx context.Context, keys ...string) error {
	return r.client.Del(ctx, keys...).Err()
}

func (r redisAdapter) DeleteIfEqual(ctx context.Context, key, expected string) (bool, error) {
	result, err := r.client.Eval(ctx, `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
`, []string{key}, expected).Int()
	return result == 1, err
}

func (r redisAdapter) ConsumeIfEqual(ctx context.Context, key, expected string) (bool, error) {
	result, err := r.client.Eval(ctx, `
local actual = redis.call('GET', KEYS[1])
if actual ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
`, []string{key}, expected).Int()
	return result == 1, err
}

func (r redisAdapter) SetIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, value, ttl).Result()
}

// DecrementIfExists is used only for the non-authoritative inventory display
// cache. It never creates a missing cache entry and never lets its value fall
// below zero.
func (r redisAdapter) DecrementIfExists(ctx context.Context, key string, amount uint64) (bool, error) {
	if key == "" || amount == 0 {
		return false, errors.New("redis inventory decrement requires a key and positive amount")
	}
	result, err := r.client.Eval(ctx, `
local current = redis.call('GET', KEYS[1])
if current == false then return 0 end
local available = tonumber(current)
local decrement = tonumber(ARGV[1])
if available == nil or decrement == nil or decrement <= 0 then
  redis.call('DEL', KEYS[1])
  return 0
end
local next = available - decrement
if next < 0 then next = 0 end
redis.call('SET', KEYS[1], tostring(next), 'KEEPTTL')
return 1
`, []string{key}, strconv.FormatUint(amount, 10)).Int()
	return result == 1, err
}

func (r redisAdapter) Increment(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	result, err := r.client.Incr(ctx, key).Result()
	if err != nil || result != 1 {
		return result, err
	}
	return result, r.client.Expire(ctx, key, ttl).Err()
}

const (
	orderWriterBatchTimeout = 5 * time.Millisecond
	orderTopicReplication   = 1
)

type kafkaProducer struct {
	brokers []string
	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func NewKafkaProducer(raw string) KafkaProducer {
	return newKafkaProducer(raw)
}

func newKafkaProducer(raw string) *kafkaProducer {
	brokers := kafkaBrokers(raw)
	return &kafkaProducer{brokers: brokers, writers: make(map[string]*kafka.Writer)}
}

func kafkaBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func (p *kafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	w, err := p.writer(topic)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value, Time: time.Now().UTC()})
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

func (p *kafkaProducer) writer(topic string) (*kafka.Writer, error) {
	if len(p.brokers) == 0 {
		return nil, errors.New("kafka broker is not configured")
	}
	if topic == "" {
		return nil, errors.New("kafka topic is not configured")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if writer := p.writers[topic]; writer != nil {
		return writer, nil
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(p.brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: orderWriterBatchTimeout,
	}
	p.writers[topic] = writer
	return writer, nil
}

func (p *kafkaProducer) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var joined error
	for topic, writer := range p.writers {
		joined = errors.Join(joined, writer.Close())
		delete(p.writers, topic)
	}
	return joined
}

// EnsureOrderTopic creates the order topic before the consumer joins its
// group. Kafka readers that subscribe before a new topic has partitions can
// receive an empty assignment and stay unable to process later messages.
func EnsureOrderTopic(ctx context.Context, rawBrokers, topic string, partitions int) error {
	brokers := kafkaBrokers(rawBrokers)
	if len(brokers) == 0 {
		return errors.New("kafka broker is not configured")
	}
	if topic == "" {
		return errors.New("kafka topic is not configured")
	}
	if partitions <= 0 {
		return errors.New("kafka topic partitions must be positive")
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		return err
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: orderTopicReplication}); err != nil {
		return err
	}
	current, err := controllerConn.ReadPartitions(topic)
	if err != nil {
		return err
	}
	if len(current) >= partitions {
		return nil
	}
	client := &kafka.Client{Addr: kafka.TCP(net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))}
	response, err := client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{Topics: []kafka.TopicPartitionsConfig{{Name: topic, Count: int32(partitions)}}})
	if err != nil {
		return err
	}
	if topicErr := response.Errors[topic]; topicErr != nil {
		return topicErr
	}
	return nil
}
