// Command server runs the rate-limit platform HTTP API.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"ratelimit-platform/internal/api"
	"ratelimit-platform/internal/config"
	"ratelimit-platform/internal/policy"
	"ratelimit-platform/internal/ratelimit"
)

func main() {
	cfg := config.FromEnv()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- Redis ---------------------------------------------------------
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("redis unreachable at %s: %v", cfg.RedisAddr, err)
	}
	engine := ratelimit.NewEngine(rdb, ratelimit.Config{
		IdemAllowTTL:   cfg.IdemAllowTTL,
		IdemDenyTTL:    cfg.IdemDenyTTL,
		DedupTTL:       cfg.DedupTTL,
		DecisionBudget: cfg.DecisionBudget,
	})

	// ---- Policy backend ------------------------------------------------
	var store policy.Store
	var pg *policy.PgStore
	switch cfg.Backend {
	case "postgres":
		var err error
		pg, err = policy.NewPgStore(ctx, cfg.PostgresDSN)
		if err != nil {
			log.Fatalf("postgres: %v", err)
		}
		store = pg
		if cfg.Seed {
			if err := policy.SeedIfEmpty(ctx, store); err != nil {
				log.Fatalf("seed: %v", err)
			}
		}
	case "memory":
		mem := policy.NewMemoryStore()
		store = mem
		if cfg.Seed {
			if err := policy.Seed(ctx, store); err != nil {
				log.Fatalf("seed: %v", err)
			}
		}
	default:
		log.Fatalf("unknown POLICY_BACKEND %q", cfg.Backend)
	}

	cached, err := policy.NewCachedStore(ctx, store, cfg.PolicyRefresh)
	if err != nil {
		log.Fatalf("policy cache: %v", err)
	}
	go cached.RunBackground(ctx)
	if pg != nil {
		go pg.RunListener(ctx)
	}

	// ---- HTTP ----------------------------------------------------------
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger())

	srv := &api.Server{
		Resolver:  cached,
		Engine:    engine,
		Clock:     func() time.Time { return time.Now().UTC() },
		IdleFloor: cfg.IdleTTLFloor,
		RedisHealth: func() error {
			c, cfn := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cfn()
			return rdb.Ping(c).Err()
		},
	}
	srv.SetRedisTimeFn(func(ctx context.Context) (time.Time, error) {
		return redisServerTime(ctx, rdb)
	})
	srv.Register(r)

	admin := &api.AdminServer{
		Store:   store,
		Refresh: func() error { return cached.Refresh(context.Background()) },
	}
	admin.RegisterAdmin(r)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("ratelimit-platform listening on %s (backend=%s)", cfg.HTTPAddr, cfg.Backend)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cfn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cfn()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if pg != nil {
		pg.Close()
	}
	_ = rdb.Close()
	os.Exit(0)
}

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		t := time.Now()
		c.Next()
		log.Printf("%s %s -> %d (%s)", c.Request.Method, c.Request.URL.Path,
			c.Writer.Status(), time.Since(t))
	}
}

func redisServerTime(ctx context.Context, rdb *redis.Client) (time.Time, error) {
	res, err := rdb.Time(ctx).Result()
	if err != nil {
		return time.Time{}, err
	}
	return res.UTC(), nil
}
