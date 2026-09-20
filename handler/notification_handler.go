package handler

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gao66666/GoBlog/database"
	"github.com/gao66666/GoBlog/jwt_module"
	"github.com/gao66666/GoBlog/models"
	"github.com/gao66666/GoBlog/mq"
	"github.com/gao66666/GoBlog/tool"
	"github.com/gin-gonic/gin"
	"github.com/olahol/melody"
	"go.uber.org/zap"
)

type NotificationHandler struct {
	Meld               *melody.Melody
	Conns              sync.Map // 存储 key: uint64(UserID), value: *melody.Session
	idleTimers         sync.Map // 存储 key: uint64(UserID), value: *time.Timer
	notificationRepo   *database.NotificationRepository
	redisNotification  *database.RedisNotificationRepository
	sessionIdleTimeout time.Duration
}

// PushNotification 在本机有该用户的活跃 WebSocket 时直接下行；返回 true 表示已成功推送。
func (nh *NotificationHandler) PushNotification(userID uint64, payload []byte) bool {
	if nh == nil || userID == 0 || len(payload) == 0 {
		return false
	}
	val, ok := nh.Conns.Load(userID)
	if !ok {
		return false
	}
	s, ok := val.(*melody.Session)
	if !ok || s == nil || s.IsClosed() {
		nh.Conns.Delete(userID)
		return false
	}
	if err := s.Write(payload); err != nil {
		nh.Conns.Delete(userID)
		return false
	}
	nh.touchWSIdleTimer(userID)
	return true
}

// NotifyPushOrStore 实现 mq.NotifySink：先尝试本机 WebSocket；不在线则异步未读 + 落库队列（Kafka 不可用时同步写库）。
func (nh *NotificationHandler) NotifyPushOrStore(userID, senderID uint64, senderName, content, msgType string) {
	if nh == nil || userID == 0 {
		return
	}
	p := mq.NotificationPayload{
		EventID:    tool.GenerateID(),
		UserID:     userID,
		SenderID:   senderID,
		SenderName: senderName,
		Content:    content,
		Type:       msgType,
		CreatedAt:  time.Now().Unix(),
	}
	nh.deliverNotificationPayload(p)
}

func (nh *NotificationHandler) deliverNotificationPayload(p mq.NotificationPayload) {
	if nh == nil || p.UserID == 0 {
		return
	}
	msg, err := json.Marshal(p)
	if err != nil {
		zap.L().Warn("通知序列化失败", zap.Error(err))
		return
	}
	if nh.PushNotification(p.UserID, msg) {
		zap.L().Info("实时推送成功", zap.Uint64("to_uid", p.UserID), zap.String("type", p.Type))
		// 在线送达后仍异步走 Kafka → 落库，记录为已读（审计/多端一致）。
		go nh.persistOnlineDeliveredArchive(p)
		return
	}
	go func() {
		nh.persistOfflineNotification(p)
	}()
}

func NewNotificationHandler(notificationRepo *database.NotificationRepository, redisNotification *database.RedisNotificationRepository) *NotificationHandler {
	m := melody.New()
	nh := &NotificationHandler{
		Meld:               m,
		notificationRepo:   notificationRepo,
		redisNotification:  redisNotification,
		sessionIdleTimeout: time.Duration(getSessionIdleTimeoutSec()) * time.Second,
	}

	// 握手成功后，自动触发这个钩子
	m.HandleConnect(func(s *melody.Session) {
		// 从我们在 HandleWS 存入的 Keys 里取值
		if uid, ok := s.Get("userID"); ok {
			// 签到：UID -> Session
			userID := uid.(uint64)
			nh.touchWSIdleTimer(userID)
			nh.Conns.Store(userID, s)
			// 只在打开 /me 时才同步离线通知（避免在其它页面登录就把通知拉走/标已读）
			if v, ok := s.Get("syncUnread"); ok {
				if b, ok := v.(bool); ok && b {
					go nh.syncUnreadNotifications(userID, s)
				}
			}
		}
	})

	m.HandleDisconnect(func(s *melody.Session) {
		if uid, ok := s.Get("userID"); ok {
			userID := uid.(uint64)
			nh.clearIdleTimer(userID)
			nh.Conns.Delete(userID)
		}
	})

	return nh
}

// internal/handler/ws_handler.go

func (nh *NotificationHandler) HandleWS(c *gin.Context) {
	// 1. 提取 Token
	token := c.Query("token")
	if token == "" {
		tool.ResponseError(c, jwt_module.ErrInvalidToken)
		return
	}

	// 2. 校验 Token
	claims, err := jwt_module.ParseToken(token)
	if err != nil {
		tool.ResponseError(c, jwt_module.ErrInvalidToken)
		return
	}

	// 3. 升级连接
	// 将解析出的 UID 和 Token 存入 Melody Session 的 Keys 中
	syncUnread := c.Query("sync") == "1"
	err = nh.Meld.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{
		"userID":     claims.UserID,
		"token":      "Bearer " + token,
		"syncUnread": syncUnread,
	})

	if err != nil {
		zap.L().Error("WebSocket 升级失败", zap.Error(err))
	}
}

// GetUserOnlineStatus 公开查询用户在线状态（WebSocket 是否存在活跃连接）。
func (nh *NotificationHandler) GetUserOnlineStatus(c *gin.Context) {
	idStr := c.Param("id")
	uid, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || uid == 0 {
		tool.ResponseError(c, ErrCodeInvalidParam)
		return
	}
	_, ok := nh.Conns.Load(uid)
	tool.ResponseSuccess(c, gin.H{"online": ok}, "ok")
}

func (nh *NotificationHandler) syncUnreadNotifications(userID uint64, session *melody.Session) {
	if nh.redisNotification == nil || nh.notificationRepo == nil {
		return
	}

	hasUnread, err := nh.redisNotification.HasUnread(userID)
	if err != nil || !hasUnread {
		return
	}

	notifications, err := nh.notificationRepo.GetUnreadByUserID(userID)
	if err != nil || len(notifications) == 0 {
		return
	}

	for _, item := range notifications {
		if session.IsClosed() {
			return
		}
		body, err := json.Marshal(item)
		if err != nil {
			zap.L().Warn("序列化离线通知失败", zap.Error(err))
			return
		}
		if err := session.Write(body); err != nil {
			zap.L().Warn("同步离线通知失败", zap.Uint64("uid", userID), zap.Error(err))
			return
		}
		nh.touchWSIdleTimer(userID)
	}

	if err := nh.redisNotification.ClearHasUnread(userID); err != nil {
		zap.L().Warn("清理未读红点失败", zap.Uint64("uid", userID), zap.Error(err))
	}

	go func() {
		if err := nh.notificationRepo.MarkReadByUserID(userID); err != nil {
			zap.L().Warn("离线通知标记已读失败", zap.Uint64("uid", userID), zap.Error(err))
		}
	}()
}

func (nh *NotificationHandler) ProcessNotificationMessage(ctx context.Context, payload []byte) error {
	var p mq.NotificationPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		zap.L().Error("解析通知 Payload 失败", zap.Error(err))
		return nil
	}
	// 与 NotifyPushOrStore 同源：兼容仍写入 notification.push 的旧生产者或外部系统。
	nh.deliverNotificationPayload(p)
	return nil
}

func (nh *NotificationHandler) persistOnlineDeliveredArchive(p mq.NotificationPayload) {
	arch := p
	arch.IsRead = true
	if err := mq.PublishNotificationStore(arch); err != nil {
		zap.L().Error("在线通知归档入队失败", zap.Uint64("to_uid", p.UserID), zap.Error(err))
		nh.persistNotificationSyncFallback(arch)
	}
}

func (nh *NotificationHandler) persistOfflineNotification(p mq.NotificationPayload) {
	if nh.redisNotification != nil {
		_ = nh.redisNotification.SetHasUnread(p.UserID)
	}
	offline := p
	offline.IsRead = false
	if err := mq.PublishNotificationStore(offline); err != nil {
		zap.L().Error("通知离线落库消息发送失败", zap.Error(err))
		nh.persistNotificationSyncFallback(offline)
	}
}

func (nh *NotificationHandler) persistNotificationSyncFallback(p mq.NotificationPayload) {
	if nh.notificationRepo == nil {
		return
	}
	now := time.Now()
	created := time.Unix(p.CreatedAt, 0)
	if p.CreatedAt == 0 {
		created = now
	}
	n := &models.Notification{
		EventID:    p.EventID,
		UserID:     p.UserID,
		SenderID:   p.SenderID,
		SenderName: p.SenderName,
		Content:    p.Content,
		Type:       p.Type,
		IsRead:     p.IsRead,
		CreatedAt:  created,
		UpdatedAt:  now,
	}
	if p.IsRead {
		t := now
		n.ReadAt = &t
	}
	if err := nh.notificationRepo.BatchCreateNotifications([]*models.Notification{n}); err != nil {
		zap.L().Warn("通知同步落库失败", zap.Uint64("to_uid", p.UserID), zap.Error(err))
	}
}

// touchWSIdleTimer 在「建连、客户端上行、服务端下行写 WS 成功」后调用，重置空闲计时；到期主动 Close，触发离线链路与前端重连。
func (nh *NotificationHandler) touchWSIdleTimer(userID uint64) {
	if nh == nil || userID == 0 || nh.sessionIdleTimeout <= 0 {
		return
	}
	if old, ok := nh.idleTimers.Load(userID); ok {
		if timer, ok := old.(*time.Timer); ok {
			timer.Stop()
		}
	}
	var t *time.Timer
	t = time.AfterFunc(nh.sessionIdleTimeout, func() {
		if cur, ok := nh.idleTimers.Load(userID); ok {
			if curT, ok := cur.(*time.Timer); !ok || curT != t {
				return
			}
			nh.idleTimers.CompareAndDelete(userID, t)
		}
		nh.forceIdleDisconnect(userID)
	})
	nh.idleTimers.Store(userID, t)
}

func (nh *NotificationHandler) forceIdleDisconnect(userID uint64) {
	if nh == nil || userID == 0 {
		return
	}
	val, ok := nh.Conns.Load(userID)
	if !ok {
		return
	}
	s, ok := val.(*melody.Session)
	if !ok || s == nil || s.IsClosed() {
		nh.Conns.Delete(userID)
		return
	}
	if err := s.Close(); err != nil {
		zap.L().Warn("空闲超时关闭 WebSocket 失败", zap.Uint64("uid", userID), zap.Error(err))
	}
}

func (nh *NotificationHandler) clearIdleTimer(userID uint64) {
	if nh == nil || userID == 0 {
		return
	}
	if old, ok := nh.idleTimers.LoadAndDelete(userID); ok {
		if timer, ok := old.(*time.Timer); ok {
			timer.Stop()
		}
	}
}

func getSessionIdleTimeoutSec() int {
	const defaultTimeoutSec = 1800
	raw := os.Getenv("WS_SESSION_IDLE_TIMEOUT_SEC")
	if raw == "" {
		return defaultTimeoutSec
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return defaultTimeoutSec
	}
	return value
}
