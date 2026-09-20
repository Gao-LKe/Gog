package app

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/auth"
	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/payment"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/protection"
	"github.com/goblog/backend/internal/realtime"
)

type App struct {
	Config config.Config
	Router *gin.Engine
}

func New(cfg config.Config, deps platform.Dependencies) *App {
	r := gin.New()
	r.Use(protection.Recovery(), protection.RequestID(), protection.Middleware(protection.NewFixedWindowLimiter(120, 60_000_000_000)))
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", readiness(deps))

	api := r.Group("/api/v1")
	api.Use(auth.Require(auth.NewJWTVerifier(cfg.AuthSecret)))
	auth.RegisterRoutes(api)
	payment.RegisterRoutes(api, payment.NewService(nil, deps.Kafka))
	realtime.RegisterRoutes(api, realtime.NewHub())
	return &App{Config: cfg, Router: r}
}

func readiness(deps platform.Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		checks := map[string]error{
			"mysql": deps.MySQL.PingContext(ctx),
			"redis": deps.Redis.Ping(ctx),
			"kafka": deps.Kafka.Ping(ctx),
		}
		failed := make([]string, 0)
		for name, err := range checks {
			if err != nil {
				failed = append(failed, name)
			}
		}
		if len(failed) > 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "failed": failed})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	}
}

func (a *App) Run() error { return a.Router.Run(a.Config.HTTPAddr) }
