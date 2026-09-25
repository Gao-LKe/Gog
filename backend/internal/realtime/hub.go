package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/auth"
	"github.com/goblog/backend/internal/store"
	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

const (
	maxMessageBytes    = 4 << 10
	recentMessageLimit = 15
	writeWait          = 5 * time.Second
	pongWait           = 60 * time.Second
	pingPeriod         = 54 * time.Second
	sendQueueSize      = 32
	fallbackCloseCode  = 4008
)

var (
	errRealtimeFull    = errors.New("realtime capacity is full")
	errRealtimeCooling = errors.New("realtime fallback cooldown is active")
)

//===========================================
//      单实例实时通信中心
//===========================================

type Options struct {
	MaxConnections   int
	FallbackCooldown time.Duration
}

type Hub struct {
	db   *gorm.DB
	opts Options

	mu        sync.RWMutex
	peers     map[string]*client
	pending   map[string]struct{}
	cooldowns map[string]time.Time
}

type client struct {
	subject       string
	conn          *websocket.Conn
	send          chan outbound
	done          chan struct{}
	closeOnce     sync.Once
	lastDelivered time.Time
}

type outbound struct {
	messageType int
	payload     []byte
	closeAfter  bool
}

type incomingMessage struct {
	Type            string `json:"type"`
	ConversationID  uint64 `json:"conversation_id"`
	ClientMessageID string `json:"client_message_id"`
	Content         string `json:"content"`
	MessageID       uint64 `json:"message_id"`
}

type messageEvent struct {
	Type            string     `json:"type"`
	ID              uint64     `json:"id"`
	ConversationID  uint64     `json:"conversation_id"`
	SenderUserID    uint64     `json:"sender_user_id"`
	RecipientUserID uint64     `json:"recipient_user_id"`
	ClientMessageID string     `json:"client_message_id"`
	Content         string     `json:"content"`
	ReceivedAt      *time.Time `json:"received_at"`
	CreatedAt       time.Time  `json:"created_at"`
}

type snapshotEvent struct {
	Type     string         `json:"type"`
	Messages []messageEvent `json:"messages"`
}

func NewHub(db *gorm.DB, options Options) *Hub {
	if options.MaxConnections <= 0 {
		options.MaxConnections = 1000
	}
	if options.FallbackCooldown <= 0 {
		options.FallbackCooldown = time.Minute
	}
	return &Hub{db: db, opts: options, peers: make(map[string]*client), pending: make(map[string]struct{}), cooldowns: make(map[string]time.Time)}
}

func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

func (peer *client) close() { peer.closeOnce.Do(func() { close(peer.done); _ = peer.conn.Close() }) }

func (peer *client) enqueue(frame outbound) bool {
	select {
	case <-peer.done:
		return false
	case peer.send <- frame:
		return true
	default:
		return false
	}
}

// reserve claims one bounded realtime seat before the WebSocket Upgrade.
// The returned client has been removed from routing and must be closed after
// releasing the hub lock.
func (h *Hub) reserve(subject string) (*client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UTC()
	if until := h.cooldowns[subject]; until.After(now) {
		return nil, errRealtimeCooling
	}
	delete(h.cooldowns, subject)
	if _, exists := h.pending[subject]; exists {
		return nil, errRealtimeFull
	}

	if current := h.peers[subject]; current != nil {
		delete(h.peers, subject)
		h.pending[subject] = struct{}{}
		return current, nil
	}
	if len(h.peers)+len(h.pending) >= h.opts.MaxConnections {
		candidate := h.oldestPeerLocked()
		if candidate == nil {
			return nil, errRealtimeFull
		}
		delete(h.peers, candidate.subject)
		h.cooldowns[candidate.subject] = now.Add(h.opts.FallbackCooldown)
		h.pending[subject] = struct{}{}
		return candidate, nil
	}
	h.pending[subject] = struct{}{}
	return nil, nil
}

func (h *Hub) oldestPeerLocked() *client {
	var oldest *client
	for _, peer := range h.peers {
		if oldest == nil || peer.lastDelivered.Before(oldest.lastDelivered) {
			oldest = peer
		}
	}
	return oldest
}

func (h *Hub) cancelReservation(subject string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.pending, subject)
}

func (h *Hub) register(subject string, peer *client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, reserved := h.pending[subject]; !reserved {
		return false
	}
	delete(h.pending, subject)
	h.peers[subject] = peer
	return true
}

func (h *Hub) remove(subject string, peer *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.peers[subject] == peer {
		delete(h.peers, subject)
	}
}

func (h *Hub) beginHTTPFallback(subject string, peer *client) {
	if !peer.enqueue(outbound{messageType: websocket.CloseMessage, payload: websocket.FormatCloseMessage(fallbackCloseCode, "realtime capacity"), closeAfter: true}) {
		peer.close()
	}
}

func (h *Hub) detachSlowPeer(subject string, peer *client) {
	h.mu.Lock()
	if h.peers[subject] == peer {
		delete(h.peers, subject)
		h.cooldowns[subject] = time.Now().UTC().Add(h.opts.FallbackCooldown)
	}
	h.mu.Unlock()
	h.beginHTTPFallback(subject, peer)
}

// Send queues a text frame for the user's only realtime device. A slow device
// is removed and falls back to HTTP without affecting persisted messages.
func (h *Hub) Send(subject string, payload []byte) error {
	h.mu.RLock()
	peer := h.peers[subject]
	h.mu.RUnlock()
	if peer == nil {
		return errors.New("subject is offline")
	}
	if !peer.enqueue(outbound{messageType: websocket.TextMessage, payload: append([]byte(nil), payload...)}) {
		h.detachSlowPeer(subject, peer)
	}
	return nil
}

func (h *Hub) writePump(peer *client) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case frame := <-peer.send:
			_ = peer.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := peer.conn.WriteMessage(frame.messageType, frame.payload); err != nil {
				h.remove(peer.subject, peer)
				peer.close()
				return
			}
			if frame.closeAfter {
				h.remove(peer.subject, peer)
				peer.close()
				return
			}
		case <-ticker.C:
			_ = peer.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := peer.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.remove(peer.subject, peer)
				peer.close()
				return
			}
		case <-peer.done:
			return
		}
	}
}

func messageView(message store.ConversationMessage) messageEvent {
	return messageEvent{Type: "message.created", ID: message.ID, ConversationID: message.ConversationID, SenderUserID: message.SenderUserID, RecipientUserID: message.RecipientUserID, ClientMessageID: message.ClientMessageID, Content: message.Content, ReceivedAt: message.ReceivedAt, CreatedAt: message.CreatedAt}
}

func (h *Hub) persistMessage(ctx context.Context, senderUserID uint64, input incomingMessage) (store.ConversationMessage, store.Conversation, error) {
	if input.Type != "message.send" || input.ConversationID == 0 || input.ClientMessageID == "" || strings.TrimSpace(input.Content) == "" {
		return store.ConversationMessage{}, store.Conversation{}, errors.New("invalid message")
	}
	if len(input.ClientMessageID) > 36 || len([]byte(input.Content)) > maxMessageBytes {
		return store.ConversationMessage{}, store.Conversation{}, errors.New("message exceeds limit")
	}
	var message store.ConversationMessage
	var conversation store.Conversation
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&conversation, input.ConversationID).Error; err != nil {
			return err
		}
		var recipientUserID uint64
		switch senderUserID {
		case conversation.PublisherUserID:
			recipientUserID = conversation.InitiatorUserID
		case conversation.InitiatorUserID:
			recipientUserID = conversation.PublisherUserID
		default:
			return errors.New("conversation access denied")
		}
		message = store.ConversationMessage{ConversationID: input.ConversationID, SenderUserID: senderUserID, RecipientUserID: recipientUserID, ClientMessageID: input.ClientMessageID, Content: input.Content}
		if err := tx.Create(&message).Error; err != nil {
			if findErr := tx.Where("conversation_id = ? AND sender_user_id = ? AND client_message_id = ?", input.ConversationID, senderUserID, input.ClientMessageID).First(&message).Error; findErr == nil {
				return nil
			}
			return err
		}
		return tx.Model(&conversation).Update("updated_at", time.Now().UTC()).Error
	})
	return message, conversation, err
}

func (h *Hub) sendMessage(ctx context.Context, subject string, input incomingMessage) (messageEvent, error) {
	senderUserID, err := strconv.ParseUint(subject, 10, 64)
	if err != nil {
		return messageEvent{}, errors.New("invalid authenticated user")
	}
	message, _, err := h.persistMessage(ctx, senderUserID, input)
	if err != nil {
		return messageEvent{}, err
	}
	event := messageView(message)
	payload, err := json.Marshal(event)
	if err != nil {
		return messageEvent{}, fmt.Errorf("encode message event: %w", err)
	}
	_ = h.Send(strconv.FormatUint(message.SenderUserID, 10), payload)
	if message.RecipientUserID != message.SenderUserID {
		_ = h.Send(strconv.FormatUint(message.RecipientUserID, 10), payload)
	}
	return event, nil
}

func (h *Hub) acknowledgeMessage(ctx context.Context, recipientUserID, messageID uint64) (store.ConversationMessage, error) {
	if messageID == 0 {
		return store.ConversationMessage{}, errors.New("invalid message acknowledgement")
	}
	var message store.ConversationMessage
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("id = ? AND recipient_user_id = ?", messageID, recipientUserID).First(&message).Error; err != nil {
			return err
		}
		if message.ReceivedAt == nil {
			now := time.Now().UTC()
			if err := tx.Model(&message).Update("received_at", now).Error; err != nil {
				return err
			}
			message.ReceivedAt = &now
		}
		return nil
	})
	if err == nil {
		h.recordCompletedDelivery(message.SenderUserID, message.RecipientUserID)
	}
	return message, err
}

func (h *Hub) recordCompletedDelivery(senderUserID, recipientUserID uint64) {
	now := time.Now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	if peer := h.peers[strconv.FormatUint(senderUserID, 10)]; peer != nil {
		peer.lastDelivered = now
	}
	if peer := h.peers[strconv.FormatUint(recipientUserID, 10)]; peer != nil {
		peer.lastDelivered = now
	}
}

func (h *Hub) recentMessages(ctx context.Context, userID uint64, limit int) ([]messageEvent, error) {
	if limit <= 0 || limit > recentMessageLimit {
		limit = recentMessageLimit
	}
	var sent, received []store.ConversationMessage
	if err := h.db.WithContext(ctx).Where("sender_user_id = ?", userID).Order("created_at DESC, id DESC").Limit(limit).Find(&sent).Error; err != nil {
		return nil, err
	}
	if err := h.db.WithContext(ctx).Where("recipient_user_id = ?", userID).Order("created_at DESC, id DESC").Limit(limit).Find(&received).Error; err != nil {
		return nil, err
	}
	all := append(sent, received...)
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].ID > all[j].ID
		}
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})
	views := make([]messageEvent, 0, limit)
	seen := make(map[uint64]struct{}, len(all))
	for _, message := range all {
		if _, exists := seen[message.ID]; exists {
			continue
		}
		seen[message.ID] = struct{}{}
		views = append(views, messageView(message))
		if len(views) == limit {
			break
		}
	}
	return views, nil
}

func (h *Hub) handleInbound(ctx context.Context, subject string, input incomingMessage) error {
	switch input.Type {
	case "message.send":
		_, err := h.sendMessage(ctx, subject, input)
		return err
	case "message.ack":
		userID, err := strconv.ParseUint(subject, 10, 64)
		if err != nil {
			return errors.New("invalid authenticated user")
		}
		_, err = h.acknowledgeMessage(ctx, userID, input.MessageID)
		return err
	default:
		return errors.New("unsupported realtime event")
	}
}

func (h *Hub) sendSnapshot(ctx context.Context, subject string, peer *client) error {
	userID, err := strconv.ParseUint(subject, 10, 64)
	if err != nil {
		return err
	}
	messages, err := h.recentMessages(ctx, userID, recentMessageLimit)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(snapshotEvent{Type: "message.snapshot", Messages: messages})
	if err != nil {
		return err
	}
	if !peer.enqueue(outbound{messageType: websocket.TextMessage, payload: payload}) {
		h.detachSlowPeer(subject, peer)
	}
	return nil
}

func (h *Hub) ServeHTTP(c *gin.Context) {
	value, ok := c.Get("auth.claims")
	claims, valid := value.(auth.Claims)
	if !ok || !valid || claims.Subject == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	evicted, err := h.reserve(claims.Subject)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, errRealtimeCooling) {
			status = http.StatusTooManyRequests
		}
		c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
		return
	}
	if evicted != nil {
		h.beginHTTPFallback(evicted.subject, evicted)
	}
	reserved := true
	defer func() {
		if reserved {
			h.cancelReservation(claims.Subject)
		}
	}()

	upgrader := websocket.Upgrader{CheckOrigin: sameOrigin, Subprotocols: []string{"bearer"}}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	peer := &client{subject: claims.Subject, conn: conn, send: make(chan outbound, sendQueueSize), done: make(chan struct{}), lastDelivered: time.Now().UTC()}
	if !h.register(claims.Subject, peer) {
		_ = conn.Close()
		return
	}
	reserved = false
	defer func() { h.remove(claims.Subject, peer); peer.close() }()
	go h.writePump(peer)
	_ = h.sendSnapshot(c.Request.Context(), claims.Subject, peer)

	conn.SetReadLimit(maxMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongWait)) })
	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}
		var input incomingMessage
		if err := json.Unmarshal(payload, &input); err != nil {
			continue
		}
		if err := h.handleInbound(c.Request.Context(), claims.Subject, input); err != nil {
			errorPayload, _ := json.Marshal(gin.H{"type": "message.error", "error": err.Error()})
			_ = peer.enqueue(outbound{messageType: websocket.TextMessage, payload: errorPayload})
		}
	}
}

func (h *Hub) RecentMessagesHTTP(c *gin.Context) {
	claims, _ := c.Get("auth.claims")
	userID, err := strconv.ParseUint(claims.(auth.Claims).Subject, 10, 64)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	messages, err := h.recentMessages(c.Request.Context(), userID, recentMessageLimit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "unable to load messages"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"messages": messages})
}

func (h *Hub) SendMessageHTTP(c *gin.Context) {
	claims, _ := c.Get("auth.claims")
	var input incomingMessage
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message"})
		return
	}
	input.Type = "message.send"
	event, err := h.sendMessage(c.Request.Context(), claims.(auth.Claims).Subject, input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, event)
}

func (h *Hub) AcknowledgeMessageHTTP(c *gin.Context) {
	claims, _ := c.Get("auth.claims")
	userID, err := strconv.ParseUint(claims.(auth.Claims).Subject, 10, 64)
	messageID, parseErr := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || parseErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message acknowledgement"})
		return
	}
	message, err := h.acknowledgeMessage(c.Request.Context(), userID, messageID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, messageView(message))
}

func RegisterRoutes(r *gin.RouterGroup, h *Hub) {
	r.GET("/realtime/ws", h.ServeHTTP)
	r.GET("/realtime/messages/recent", h.RecentMessagesHTTP)
	r.POST("/realtime/messages", h.SendMessageHTTP)
	r.POST("/realtime/messages/:id/ack", h.AcknowledgeMessageHTTP)
}
