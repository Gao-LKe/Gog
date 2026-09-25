package main

import (
	"context"
	"log"
	"time"

	"github.com/goblog/backend/internal/app"
	"github.com/goblog/backend/internal/auth"
	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/store"
)

func main() {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}
	clients, err := platform.Open(cfg.MySQLDSN, cfg.RedisAddr, cfg.KafkaBroker)
	if err != nil {
		log.Fatal(err)
	}
	defer clients.Close()
	if err := store.Migrate(clients.Gorm); err != nil {
		log.Fatal(err)
	}
	if err := auth.BootstrapAdmin(context.Background(), clients.Gorm, clients.Redis, clients.RedisAtomic, cfg.BootstrapAdminEmail); err != nil {
		log.Fatal(err)
	}
	a := app.New(cfg, clients.Dependencies)
	go consumeOrders(context.Background(), a.OrderService, cfg.KafkaBroker)
	go expirePendingOrders(context.Background(), a.OrderService)
	log.Printf("api listening on %s", a.Config.HTTPAddr)
	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}

func consumeOrders(ctx context.Context, service interface {
	Consume(context.Context, string) error
}, brokers string) {
	for {
		if err := service.Consume(ctx, brokers); err != nil && ctx.Err() == nil {
			log.Printf("order consumer stopped: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		return
	}
}

func expirePendingOrders(ctx context.Context, service interface {
	ExpirePending(context.Context, int) error
}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		if err := service.ExpirePending(ctx, 100); err != nil && ctx.Err() == nil {
			log.Printf("order expiration sweep failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
