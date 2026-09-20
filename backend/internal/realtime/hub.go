package realtime

import (
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/auth"
	"github.com/gorilla/websocket"
)

type Hub struct {
	mu    sync.RWMutex
	peers map[string]map[*client]struct{}
}

type client struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func NewHub() *Hub { return &Hub{peers: make(map[string]map[*client]struct{})} }

func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

func (h *Hub) add(subject string, peer *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.peers[subject] == nil {
		h.peers[subject] = make(map[*client]struct{})
	}
	h.peers[subject][peer] = struct{}{}
}

func (h *Hub) remove(subject string, peer *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.peers[subject], peer)
	if len(h.peers[subject]) == 0 {
		delete(h.peers, subject)
	}
}

// Send is the small delivery boundary used by future Kafka consumers.
func (h *Hub) Send(subject string, payload []byte) error {
	h.mu.RLock()
	connections := make([]*client, 0, len(h.peers[subject]))
	for peer := range h.peers[subject] {
		connections = append(connections, peer)
	}
	h.mu.RUnlock()
	if len(connections) == 0 {
		return errors.New("subject is offline")
	}
	for _, peer := range connections {
		peer.mu.Lock()
		_ = peer.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		err := peer.conn.WriteMessage(websocket.TextMessage, payload)
		peer.mu.Unlock()
		if err != nil {
			h.remove(subject, peer)
			_ = peer.conn.Close()
		}
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
	upgrader := websocket.Upgrader{CheckOrigin: sameOrigin}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	peer := &client{conn: conn}
	h.add(claims.Subject, peer)
	defer func() {
		h.remove(claims.Subject, peer)
		_ = conn.Close()
	}()
	conn.SetReadLimit(4 << 10)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func RegisterRoutes(r *gin.RouterGroup, h *Hub) {
	r.GET("/realtime/ws", h.ServeHTTP)
}
