package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"example.com/ratelimit/internal/api"
	"example.com/ratelimit/internal/limiter"
	"example.com/ratelimit/internal/policy"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		port      = env("PORT", "8080")
		redisAddr = env("REDIS_ADDR", "127.0.0.1:6379")
		dsn       = env("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5432/ratelimit?sslmode=disable")
	)

	ctx := context.Background()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}
	if err := policy.Migrate(ctx, db); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	store := policy.NewPgStore(db)
	if err := policy.SeedDefaults(ctx, store); err != nil {
		log.Fatalf("seed defaults: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("warn: redis ping failed: %v (per-endpoint fail_open policy applies)", err)
	}

	srv := api.NewServer(api.Options{
		Limiter:   limiter.New(rdb),
		Store:     store,
		RedisTime: func(ctx context.Context) (time.Time, error) { return rdb.Time(ctx).Result() },
	})

	httpSrv := &http.Server{Addr: ":" + port, Handler: srv.Handler()}
	go func() {
		log.Printf("listening on :%s", port)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
