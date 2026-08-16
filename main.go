package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/heartlezz7/idempotent/src/handler"
	"github.com/heartlezz7/idempotent/src/route/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://lab:lab@localhost:5432/lab?sslmode=disable"
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
	r.POST("/_sweep", h.Sweep)
	r.POST("/_expire_keys", h.ExpireKeys)

	// ===================================================================
	//  /naive  -- no protection anywhere. This is the control group.
	// ===================================================================
	n := r.Group("/naive")
	{
		n.GET("/users", h.ListUsers)
		n.GET("/users/:id", h.GetUser)
		n.POST("/users", h.CreateUser)          // duplicates on every retry
		n.PUT("/users/:id", h.PutUserNaive)     // upsert that still bumps version
		n.PATCH("/users/:id", h.PatchUserNaive) // accumulating patch
		n.DELETE("/users/:id", h.DeleteUserNaive)

		n.GET("/information", h.ListInformation)
		n.GET("/information/:id", h.GetInformation)
		n.POST("/information", h.CreateInformationNaive)     // 500 on the retry
		n.PATCH("/information/:id", h.PatchInformationNaive) // age = age + n
		n.DELETE("/information/:id", h.DeleteInformationNaive)
	}

	// ===================================================================
	//  /safe  -- each method protected by the mechanism that suits it.
	//
	//  Note POST /users uses the SAME handler as /naive. The only
	//  difference is the middleware in front of it.
	// ===================================================================
	s := r.Group("/safe")
	{
		s.GET("/users", h.ListUsers)
		s.GET("/users/:id", h.GetUser)

		// Mechanism: Idempotency-Key. Needed because users have no
		// naturally unique field to constrain on.
		s.POST("/users", middleware.Middleware(pool), h.CreateUser)

		// Mechanism: assignment + content hash. No key required.
		s.PUT("/users/:id", h.PutUserSafe)

		// Mechanism: merge patch + If-Match.
		s.PATCH("/users/:id", h.PatchUserSafe)

		// Mechanism: guarded state transition + tombstone.
		s.DELETE("/users/:id", h.DeleteUserSafe)

		s.GET("/information", h.ListInformation)
		s.GET("/information/:id", h.GetInformation)

		// Mechanism: unique constraint. No key table at all.
		s.POST("/information", h.CreateInformationSafe)

		s.PUT("/information/:id", h.PutInformationSafe)
		s.PATCH("/information/:id", h.PatchInformationSafe)
		s.DELETE("/information/:id", h.DeleteInformationSafe)
	}

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("idempotency lab listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatal(err)
	}
}
