package app

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/auth"
	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/contracts"
	"github.com/goblog/backend/internal/monitoring"
	"github.com/goblog/backend/internal/order"
	"github.com/goblog/backend/internal/payment"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/protection"
	"github.com/goblog/backend/internal/realtime"
)

type App struct {
	Config       config.Config
	Router       *gin.Engine
	OrderService *order.Service
	Metrics      *monitoring.Metrics
	deps         platform.Dependencies
}

func New(cfg config.Config, deps platform.Dependencies) *App {
	r := gin.New()
	metrics := monitoring.NewMetrics()
	r.Use(metrics.HTTPMiddleware(), protection.Recovery(), protection.RequestID(), protection.Middleware(protection.NewFixedWindowLimiter(120, 60_000_000_000)))
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", readiness(deps))

	emailSender := auth.NewEmailSender(auth.EmailConfig{
		Mode: cfg.EmailMode, Host: cfg.SMTPHost, Port: cfg.SMTPPort, Username: cfg.SMTPUsername, Password: cfg.SMTPPassword, From: cfg.SMTPFrom,
	})
	authService := auth.NewService(deps.Gorm, deps.Redis, deps.RedisAtomic, emailSender, auth.ServiceConfig{
		Secret: cfg.AuthSecret, Issuer: cfg.AuthIssuer, Audience: cfg.AuthAudience,
		AccessTokenTTL: cfg.AccessTokenTTL, RefreshTokenTTL: cfg.RefreshTokenTTL, EmailCodeTTL: cfg.EmailCodeTTL, BootstrapAdminEmail: cfg.BootstrapAdminEmail, SnowflakeNodeID: cfg.SnowflakeNodeID,
	})
	publicAuth := r.Group("/api/v1/auth")
	auth.RegisterPublicRoutes(publicAuth, authService)

	api := r.Group("/api/v1")
	api.Use(auth.Require(auth.NewJWTVerifierWithScope(cfg.AuthSecret, cfg.AuthIssuer, cfg.AuthAudience), authService))
	auth.RegisterRoutes(api.Group("/auth"), authService)
	orderService := order.NewService(deps.Gorm, deps.Redis, deps.Kafka, metrics)
	order.RegisterRoutes(api, orderService)
	payment.RegisterRoutes(api, payment.NewService(deps.Gorm))
	realtime.RegisterRoutes(api, realtime.NewHub(deps.Gorm, realtime.Options{MaxConnections: cfg.RealtimeMaxConnections, FallbackCooldown: cfg.RealtimeFallbackCooldown}))
	monitor := api.Group("/monitoring")
	monitor.Use(auth.RequireEndpoint(auth.DefaultEndpointAuthorizer(), auth.EndpointRule{Role: auth.RoleAdmin, Permission: auth.PermissionManageSystem}))
	reader := monitoring.NewReader(cfg.PrometheusURL, metrics)
	monitor.GET("/overview", func(c *gin.Context) {
		window := c.DefaultQuery("window", "5m")
		if _, ok := monitoring.ValidWindow(window); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid monitoring window"})
			return
		}
		view, err := reader.Overview(c.Request.Context(), window)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "monitoring data unavailable"})
			return
		}
		c.JSON(http.StatusOK, view)
	})
	return &App{Config: cfg, Router: r, OrderService: orderService, Metrics: metrics, deps: deps}
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

func (a *App) Run() error {
	listener, err := net.Listen("tcp", a.Config.MetricsAddr)
	if err != nil {
		return err
	}
	metricsServer := &http.Server{Handler: a.Metrics.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	go a.sample(ctx)
	defer func() {
		cancel()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()
	return a.Router.Run(a.Config.HTTPAddr)
}

func (a *App) sample(ctx context.Context) {
	var db = a.deps.Gorm
	var rawBrokers = strings.Split(a.Config.KafkaBroker, ",")
	brokers := make([]string, 0, len(rawBrokers))
	for _, broker := range rawBrokers {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		if db != nil {
			if sqlDB, err := db.DB(); err == nil {
				a.Metrics.SampleDB(sqlDB)
				pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				a.Metrics.SetDBProbe(sqlDB.PingContext(pingCtx) == nil)
				cancel()
			}
		}
		snapshot, err := monitoring.SampleKafkaLag(ctx, brokers, contracts.OrderCreationTopic, order.ConsumerGroupID, 3*time.Hour)
		if err != nil {
			snapshot.State = "unavailable"
		}
		a.Metrics.SetKafkaSample(snapshot)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
