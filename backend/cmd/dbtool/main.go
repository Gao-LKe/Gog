package main

import (
	"flag"
	"log"

	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/store"
)

func main() {
	action := flag.String("action", "migrate", "执行动作：migrate 或 seed")
	flag.Parse()

	db, err := store.OpenMySQL(config.FromEnv().MySQLDSN)
	if err != nil {
		log.Fatal(err)
	}

	if err := store.Migrate(db); err != nil {
		log.Fatal(err)
	}
	if *action == "migrate" {
		log.Print("database migration completed")
		return
	}
	if *action != "seed" {
		log.Fatalf("unsupported action: %s", *action)
	}
	if err := store.SeedMockData(db); err != nil {
		log.Fatal(err)
	}
	log.Print("mock data seeded")
}
