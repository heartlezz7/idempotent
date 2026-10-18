package main

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/heartlezz7/idempotent/docs/swagger"
	"github.com/heartlezz7/idempotent/src/handler"
	"github.com/heartlezz7/idempotent/src/route/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title			Idempotency Lab API
// @version		1.0
// @description	Two parallel route trees over the same two resources.
// @description	/naive has no protection; /safe protects each method with the mechanism that suits it.
// @description	Every mutation appends to audit_log -- read GET /_stats to see whether a retry duplicated anything.
// @host			localhost:8080
// @BasePath		/
// @schemes		http
func main() {
	// Load .env from the working directory if there is one. Variables
	// already set in the environment (e.g. by docker compose) win, and a
	// missing file is fine -- the defaults below still apply.
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Fatalf("load .env: %v", err)
	}

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

	// ---- API docs: /swagger/index.html ---------------------------------
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

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
