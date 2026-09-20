package protection

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type Limiter interface {
	Allow(key string) bool
}

type bucket struct {
	started time.Time
	count   int
}

// FixedWindowLimiter is deliberately small; distributed deployments can
// replace it with a Redis-backed implementation through the Limiter interface.
type FixedWindowLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	buckets map[string]bucket
}

func NewFixedWindowLimiter(limit int, window time.Duration) *FixedWindowLimiter {
	if limit < 1 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &FixedWindowLimiter{limit: limit, window: window, buckets: make(map[string]bucket)}
}

func (l *FixedWindowLimiter) Allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b.started.IsZero() || now.Sub(b.started) >= l.window {
		b = bucket{started: now}
	}
	if b.count >= l.limit {
		return false
	}
	b.count++
	l.buckets[key] = b
	return true
}

func Middleware(l Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		if l != nil && !l.Allow(c.ClientIP()) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = time.Now().UTC().Format("20060102T150405.000000000Z07:00")
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

func Recovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, _ any) {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	})
}
