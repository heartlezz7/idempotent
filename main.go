package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/heartlezz7/idempotent/src/handler"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v (is docker compose up?)", err)
	}

	h := handler.New(pool)
	r := gin.Default()

	// ---- lab controls -------------------------------------------------
	r.GET("/_stats", h.Stats)
	r.POST("/_reset", h.Reset)

	// ---- producer -------------------------------------------------------
	// A manual stand-in for an external system publishing onto the
	// simulated broker (topics: "orders", "notifications").
	r.POST("/publish", h.Publish)

	// ---- consumers --------------------------------------------------
	// Same two topics, two consumer implementations each: /naive has no
	// protection and duplicates every redelivery; /safe applies the
	// mechanism that fits the resource -- a monotonic guard for orders
	// (naturally idempotent, no dedup table), the inbox pattern for
	// notifications (not naturally idempotent, needs one).
	r.POST("/naive/consume", h.NaiveConsume)
	r.POST("/safe/consume", h.SafeConsume)

	// ---- transactional outbox relay ----------------------------------
	// Turns outbox_events rows written by the safe order consumer into
	// real "notifications" messages on the queue.
	r.POST("/relay/run", h.RelayRun)

	// ---- chaos / lab-only controls ------------------------------------
	r.POST("/_duplicate", h.ForceRedeliver)
	r.POST("/_expire_lease", h.ForceExpireLease)

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("idempotency lab (async/queue flow) listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatal(err)
	}
}
