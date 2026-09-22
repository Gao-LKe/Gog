package main

import (
	"log"

	"github.com/goblog/backend/internal/app"
	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/store"
)

func main() {
	cfg := config.FromEnv()
	clients, err := platform.Open(cfg.MySQLDSN, cfg.RedisAddr, cfg.KafkaBroker)
	if err != nil {
		log.Fatal(err)
	}
	defer clients.Close()
	if err := store.Migrate(clients.Gorm); err != nil {
		log.Fatal(err)
	}
	a := app.New(cfg, clients.Dependencies)
	log.Printf("api listening on %s", a.Config.HTTPAddr)
	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}
