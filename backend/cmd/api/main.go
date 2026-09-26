package main

import (
	"context"
	"log"
	"time"

	"github.com/goblog/backend/internal/app"
	"github.com/goblog/backend/internal/auth"
	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/contracts"
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
	topicCtx, cancelTopic := context.WithTimeout(context.Background(), 10*time.Second)
	if err := platform.EnsureOrderTopic(topicCtx, cfg.KafkaBroker, contracts.OrderCreationTopic, cfg.OrderTopicPartitions); err != nil {
		cancelTopic()
		log.Fatal(err)
	}
	cancelTopic()
	if err := auth.BootstrapAdmin(context.Background(), clients.Gorm, clients.Redis, clients.RedisAtomic, cfg.BootstrapAdminEmail); err != nil {
		log.Fatal(err)
	}
	a := app.New(cfg, clients.Dependencies)
	startOrderConsumers(context.Background(), a.OrderService, cfg.KafkaBroker, cfg.OrderConsumerWorkers)
	go expirePendingOrders(context.Background(), a.OrderService)
	log.Printf("api listening on %s", a.Config.HTTPAddr)
	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}

func startOrderConsumers(ctx context.Context, service interface {
	Consume(context.Context, string) error
}, brokers string, workers int) {
	for worker := 0; worker < workers; worker++ {
		go consumeOrders(ctx, service, brokers)
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
	ExpirePending(context.Context, int) (int, error)
}) {
	const (
		batchSize = 500
		interval  = time.Second
	)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		expired, err := service.ExpirePending(ctx, batchSize)
		if err != nil && ctx.Err() == nil {
			log.Printf("order expiration sweep failed: %v", err)
		}
		// A full batch means more orders may already be overdue. Drain it before
		// waiting for the next cadence so a short sales spike does not lock stock.
		if err == nil && expired == batchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
