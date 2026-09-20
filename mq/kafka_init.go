package mq

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"strconv"

	"github.com/gao66666/GoBlog/models"
	"github.com/gao66666/GoBlog/tool"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	TopicNotificationPush  = "notification.push"
	TopicNotificationStore = "notification.store"
	TopicSearchPush        = "search.push"
	TopicPointsEarn        = "points.earn"
	TopicPointsEarnDLQ     = "points.earn.dlq"
)

type NotificationPayload struct {
	EventID    uint64 `json:"event_id"`
	UserID     uint64 `json:"user_id"`
	SenderID   uint64 `json:"sender_id"`
	SenderName string `json:"sender_name"`
	Content    string `json:"content"`
	Type       string `json:"type"` // follow, like, system, comment, reply
	CreatedAt  int64  `json:"created_at"`
	// IsRead 为 true 时表示已通过 WebSocket 送达，落库时记为已读（离线通知默认 false）。
	IsRead bool `json:"is_read,omitempty"`
}

// NotifySink 用户通知统一出口：先检测本机 WebSocket，在线则实时推送；不在线则异步落库。
// 多实例部署时，仅持有该用户 WS 的节点会直推；其余节点会像离线一样走存储，避免丢通知。
type NotifySink interface {
	NotifyPushOrStore(userID, senderID uint64, senderName, content, msgType string)
}

type NotificationProcessor interface {
	ProcessNotificationMessage(ctx context.Context, payload []byte) error
}

type NotificationStoreProcessor interface {
	ProcessNotificationStoreMessage(ctx context.Context, payload []byte) error
}

type SearchSyncProcessor interface {
	ProcessSearchSyncMessage(ctx context.Context, payload []byte) error
}

// PointsEarnPayload 积分入账消息（消费端幂等键为 TxnID）。
type PointsEarnPayload struct {
	UserID  uint64 `json:"user_id"`
	RefType string `json:"ref_type"`
	RefID   uint64 `json:"ref_id"`
	TxnID   uint64 `json:"txn_id"`
}

// PointsEarnProcessor Kafka 消费：入账 + 记流水。
type PointsEarnProcessor interface {
	ProcessPointsEarnMessage(ctx context.Context, payload []byte) error
}

// PointsEarnDLQEnvelope 积分入账死信消息体（人工补单或对账用）。
type PointsEarnDLQEnvelope struct {
	SourceTopic  string          `json:"source_topic"`
	Attempts     int             `json:"attempts"`
	LastError    string          `json:"last_error"`
	FailedAtUnix int64           `json:"failed_at_unix"`
	Payload      json.RawMessage `json:"payload"`
}

var (
	kafkaBrokers []string
	kafkaGroupID string

	kafkaCtx    context.Context
	kafkaCancel context.CancelFunc
	kafkaWG     sync.WaitGroup

	notificationWriter      *kafka.Writer
	notificationStoreWriter *kafka.Writer
	searchWriter            *kafka.Writer
	pointsEarnWriter        *kafka.Writer
	pointsDLQWriter         *kafka.Writer
)

// pointsEarnConsumerMaxRetries 单条 points.earn 消息最大处理尝试次数（由 InitKafka 注入，默认 3）。
var pointsEarnConsumerMaxRetries int

func resolveBrokers(brokers []string) []string {
	resolved := make([]string, 0, len(brokers))
	for _, b := range brokers {
		host, port, err := net.SplitHostPort(b)
		if err != nil {
			resolved = append(resolved, b)
			continue
		}
		// 已经是 IP 地址，不需要解析
		if net.ParseIP(host) != nil {
			resolved = append(resolved, b)
			continue
		}
		// 尝试 DNS 解析，失败则降级到 127.0.0.1
		_, err = net.LookupHost(host)
		if err != nil {
			zap.L().Info("Kafka broker host 解析失败，降级到 127.0.0.1",
				zap.String("original", b), zap.Error(err))
			resolved = append(resolved, "127.0.0.1:"+port)
			continue
		}
		resolved = append(resolved, b)
	}
	zap.L().Info("Kafka brokers 解析结果",
		zap.Strings("original", brokers),
		zap.Strings("resolved", resolved))
	return resolved
}

func InitKafka(brokers []string, groupID string, pointsEarnMaxRetriesArg int) error {
	if len(brokers) == 0 {
		return errors.New("kafka brokers is empty")
	}
	if groupID == "" {
		return errors.New("kafka groupID is empty")
	}
	if pointsEarnMaxRetriesArg <= 0 {
		pointsEarnMaxRetriesArg = 3
	}
	pointsEarnConsumerMaxRetries = pointsEarnMaxRetriesArg

	brokers = resolveBrokers(brokers)

	kafkaBrokers = append([]string(nil), brokers...)
	kafkaGroupID = groupID

	if kafkaCancel != nil {
		kafkaCancel()
	}
	kafkaCtx, kafkaCancel = context.WithCancel(context.Background())

	notificationWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        TopicNotificationPush,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 50 * time.Millisecond,
	}
	notificationStoreWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        TopicNotificationStore,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 100 * time.Millisecond,
	}
	searchWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        TopicSearchPush,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 100 * time.Millisecond,
	}
	pointsEarnWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        TopicPointsEarn,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 50 * time.Millisecond,
	}
	pointsDLQWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        TopicPointsEarnDLQ,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 50 * time.Millisecond,
	}
	return nil
}

func CloseKafka() {
	if kafkaCancel != nil {
		kafkaCancel()
		kafkaCancel = nil
	}
	kafkaWG.Wait()

	if notificationWriter != nil {
		_ = notificationWriter.Close()
		notificationWriter = nil
	}
	if notificationStoreWriter != nil {
		_ = notificationStoreWriter.Close()
		notificationStoreWriter = nil
	}
	if searchWriter != nil {
		_ = searchWriter.Close()
		searchWriter = nil
	}
	if pointsEarnWriter != nil {
		_ = pointsEarnWriter.Close()
		pointsEarnWriter = nil
	}
	if pointsDLQWriter != nil {
		_ = pointsDLQWriter.Close()
		pointsDLQWriter = nil
	}
}

func StartKafkaConsumers(notification NotificationProcessor, notificationStore NotificationStoreProcessor, search SearchSyncProcessor, points PointsEarnProcessor) error {
	if kafkaCancel == nil || kafkaCtx == nil {
		return errors.New("kafka is not initialized")
	}

	if notification != nil {
		kafkaWG.Add(1)
		go func() {
			defer kafkaWG.Done()
			consumeLoop(kafkaCtx, kafkaBrokers, kafkaGroupID, TopicNotificationPush, func(ctx context.Context, value []byte) error {
				return notification.ProcessNotificationMessage(ctx, value)
			})
		}()
	}
	if notificationStore != nil {
		kafkaWG.Add(1)
		go func() {
			defer kafkaWG.Done()
			consumeLoop(kafkaCtx, kafkaBrokers, kafkaGroupID, TopicNotificationStore, func(ctx context.Context, value []byte) error {
				return notificationStore.ProcessNotificationStoreMessage(ctx, value)
			})
		}()
	}
	if search != nil {
		kafkaWG.Add(1)
		go func() {
			defer kafkaWG.Done()
			consumeLoop(kafkaCtx, kafkaBrokers, kafkaGroupID, TopicSearchPush, func(ctx context.Context, value []byte) error {
				return search.ProcessSearchSyncMessage(ctx, value)
			})
		}()
	}
	if points != nil {
		kafkaWG.Add(1)
		go func() {
			defer kafkaWG.Done()
			consumeLoopPointsEarn(kafkaCtx, kafkaBrokers, kafkaGroupID, TopicPointsEarn, pointsEarnConsumerMaxRetries, func(ctx context.Context, value []byte) error {
				return points.ProcessPointsEarnMessage(ctx, value)
			})
		}()
	}
	return nil
}

// pointsProcessorRetrySleep 第 failedAttempt 次失败后、下一次重试前的指数退避（基数 500ms，即 500ms、1s、2s… 上限 30s）。
func pointsProcessorRetrySleep(failedAttempt int) {
	if failedAttempt < 1 {
		failedAttempt = 1
	}
	shift := failedAttempt - 1
	if shift > 6 {
		shift = 6
	}
	backoff := 500 * time.Millisecond
	if shift > 0 {
		backoff *= time.Duration(1 << shift)
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	time.Sleep(backoff)
}

func publishPointsEarnDLQ(m kafka.Message, sourceTopic string, lastErr error, attempts int) error {
	if pointsDLQWriter == nil {
		return errors.New("kafka points dlq writer is not initialized")
	}
	msg := lastErr.Error()
	if msg == "" {
		msg = "unknown error"
	}
	env := PointsEarnDLQEnvelope{
		SourceTopic:  sourceTopic,
		Attempts:     attempts,
		LastError:    msg,
		FailedAtUnix: time.Now().Unix(),
		Payload:      json.RawMessage(append([]byte(nil), m.Value...)),
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	key := append([]byte(nil), m.Key...)
	if len(key) == 0 {
		key = []byte(strconv.Itoa(m.Partition) + ":" + strconv.FormatInt(m.Offset, 10))
	}
	return pointsDLQWriter.WriteMessages(context.Background(), kafka.Message{Key: key, Value: b})
}

// consumeLoopPointsEarn 积分入账：有限次重试 + 失败入 DLQ 后提交 offset，避免永久卡分区。
func consumeLoopPointsEarn(ctx context.Context, brokers []string, groupID string, sourceTopic string, maxRetries int, processor func(context.Context, []byte) error) {
	if maxRetries < 1 {
		maxRetries = 1
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          sourceTopic,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer r.Close()

	var (
		consecutiveFetchErr int
		lastErrLogAt        time.Time
		cursorValid         bool
		curPartition        int
		curOffset           int64
		exhausted           bool
		failedAttempts      int
		lastProcErr         error
	)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			consecutiveFetchErr++
			now := time.Now()
			if lastErrLogAt.IsZero() || now.Sub(lastErrLogAt) >= 10*time.Second {
				if consecutiveFetchErr <= 1 {
					zap.L().Warn("Kafka fetch message failed", zap.String("topic", sourceTopic), zap.Error(err))
				} else {
					zap.L().Info("Kafka fetch retrying", zap.String("topic", sourceTopic), zap.Error(err))
				}
				lastErrLogAt = now
			}
			backoff := 500 * time.Millisecond
			shift := consecutiveFetchErr - 1
			if shift > 6 {
				shift = 6
			}
			if shift > 0 {
				backoff = backoff * time.Duration(1<<shift)
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			time.Sleep(backoff)
			continue
		}
		consecutiveFetchErr = 0

		if !cursorValid || curPartition != m.Partition || curOffset != m.Offset {
			cursorValid = true
			curPartition = m.Partition
			curOffset = m.Offset
			exhausted = false
			failedAttempts = 0
			lastProcErr = nil
		}

		if !exhausted {
			procErr := processor(ctx, m.Value)
			if procErr == nil {
				if err := r.CommitMessages(ctx, m); err != nil {
					zap.L().Warn("Kafka commit failed", zap.String("topic", sourceTopic), zap.Error(err))
				}
				cursorValid = false
				continue
			}
			lastProcErr = procErr
			failedAttempts++
			zap.L().Warn("Kafka points earn process failed",
				zap.String("topic", sourceTopic),
				zap.Int("partition", m.Partition),
				zap.Int64("offset", m.Offset),
				zap.Int("attempt", failedAttempts),
				zap.Int("max_retries", maxRetries),
				zap.Error(procErr))

			if failedAttempts < maxRetries {
				pointsProcessorRetrySleep(failedAttempts)
				continue
			}
			exhausted = true
		}

		if lastProcErr == nil {
			lastProcErr = errors.New("max retries exceeded")
		}
		if err := publishPointsEarnDLQ(m, sourceTopic, lastProcErr, failedAttempts); err != nil {
			zap.L().Error("points earn DLQ publish failed", zap.String("topic", sourceTopic), zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		zap.L().Error("points earn sent to DLQ after max retries",
			zap.String("source_topic", sourceTopic),
			zap.Int("partition", m.Partition),
			zap.Int64("offset", m.Offset),
			zap.Int("attempts", failedAttempts),
			zap.Error(lastProcErr))
		if err := r.CommitMessages(ctx, m); err != nil {
			zap.L().Warn("Kafka commit failed after DLQ", zap.String("topic", sourceTopic), zap.Error(err))
		}
		cursorValid = false
		exhausted = false
		failedAttempts = 0
		lastProcErr = nil
	}
}

func consumeLoop(ctx context.Context, brokers []string, groupID string, topic string, processor func(context.Context, []byte) error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: 0, // 手动提交，确保处理成功后再 commit
	})
	defer r.Close()

	var (
		consecutiveFetchErr int
		lastErrLogAt        time.Time
	)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			consecutiveFetchErr++
			now := time.Now()
			if lastErrLogAt.IsZero() || now.Sub(lastErrLogAt) >= 10*time.Second {
				// 第一次用 WARN，后续用 INFO（避免 Kafka 未启动时刷屏）
				if consecutiveFetchErr <= 1 {
					zap.L().Warn("Kafka fetch message failed", zap.String("topic", topic), zap.Error(err))
				} else {
					zap.L().Info("Kafka fetch retrying", zap.String("topic", topic), zap.Error(err))
				}
				lastErrLogAt = now
			}

			backoff := 500 * time.Millisecond
			// 指数退避，上限 30s
			shift := consecutiveFetchErr - 1
			if shift > 6 {
				shift = 6
			}
			if shift > 0 {
				backoff = backoff * time.Duration(1<<shift)
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			time.Sleep(backoff)
			continue
		}
		consecutiveFetchErr = 0

		if err := processor(ctx, m.Value); err != nil {
			zap.L().Error("Kafka message process failed", zap.String("topic", topic), zap.Error(err))
			// 不 commit，让其后续重试（可能会导致重复投递处理，业务需幂等）
			time.Sleep(1 * time.Second)
			continue
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			zap.L().Warn("Kafka commit failed", zap.String("topic", topic), zap.Error(err))
		}
	}
}

// PublishNotification 写入 Kafka topic notification.push（兼容外部/旧生产者）。
// 站内业务请使用 NotifySink.NotifyPushOrStore：先本机 WebSocket，离线再异步落库。
func PublishNotification(userID uint64, senderID uint64, senderName, content string, msgType string) error {
	if notificationWriter == nil {
		return errors.New("kafka notification writer is not initialized")
	}

	payload, err := json.Marshal(NotificationPayload{
		EventID:    tool.GenerateID(),
		UserID:     userID,
		SenderID:   senderID,
		SenderName: senderName,
		Content:    content,
		Type:       msgType,
		CreatedAt:  time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	return notificationWriter.WriteMessages(context.Background(), kafka.Message{Value: payload})
}

func PublishNotificationStore(data NotificationPayload) error {
	if notificationStoreWriter == nil {
		return errors.New("kafka notification store writer is not initialized")
	}
	if data.EventID == 0 {
		data.EventID = tool.GenerateID()
	}
	if data.CreatedAt == 0 {
		data.CreatedAt = time.Now().Unix()
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return notificationStoreWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(strconv.FormatUint(data.UserID, 10)), Value: payload})
}

func PublishSearchSync(data *models.SearchSyncPayload) error {
	if searchWriter == nil {
		return errors.New("kafka search writer is not initialized")
	}
	if data == nil {
		return errors.New("search sync payload is nil")
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	key := []byte("")
	if data.ID != 0 {
		key = []byte(strconv.FormatUint(data.ID, 10))
	}
	return searchWriter.WriteMessages(context.Background(), kafka.Message{Key: key, Value: payload})
}

// PointsEarnKafkaAvailable 已初始化 Kafka 且具备积分入账 writer（与是否启用消费者独立，由 InitKafka 决定）。
func PointsEarnKafkaAvailable() bool {
	return pointsEarnWriter != nil
}

// PublishPointsEarn 投递积分入账；Key 使用 txn_id 便于分区内有序。
func PublishPointsEarn(p *PointsEarnPayload) error {
	if pointsEarnWriter == nil {
		return errors.New("kafka points earn writer is not initialized")
	}
	if p == nil || p.UserID == 0 || p.RefType == "" || p.TxnID == 0 {
		return errors.New("invalid points earn payload")
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	key := []byte(strconv.FormatUint(p.TxnID, 10))
	return pointsEarnWriter.WriteMessages(context.Background(), kafka.Message{Key: key, Value: payload})
}
