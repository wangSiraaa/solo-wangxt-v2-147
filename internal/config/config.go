// Package config loads runtime configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config is the process configuration.
type Config struct {
	HTTPAddr string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	PostgresDSN string

	// Backend selects policy persistence: "memory" or "postgres".
	Backend string

	PolicyRefresh  time.Duration
	DecisionBudget time.Duration

	IdemAllowTTL time.Duration
	IdemDenyTTL  time.Duration
	DedupTTL     time.Duration

	// IdleTTLFloor / multiplier govern bucket key lifetimes.
	IdleTTLFloor time.Duration

	Seed bool
}

// FromEnv loads configuration with defaults suitable for local development.
func FromEnv() Config {
	c := Config{
		HTTPAddr:       getenv("HTTP_ADDR", ":8080"),
		RedisAddr:      getenv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:  os.Getenv("REDIS_PASSWORD"),
		RedisDB:        getenvInt("REDIS_DB", 0),
		PostgresDSN:    os.Getenv("POSTGRES_DSN"),
		Backend:        getenv("POLICY_BACKEND", "memory"),
		PolicyRefresh:  getenvDur("POLICY_REFRESH", 30*time.Second),
		DecisionBudget: getenvDur("DECISION_BUDGET", 200*time.Millisecond),
		IdemAllowTTL:   getenvDur("IDEM_ALLOW_TTL", 60*time.Second),
		IdemDenyTTL:    getenvDur("IDEM_DENY_TTL", 5*time.Second),
		DedupTTL:       getenvDur("DEDUP_TTL", 24*time.Hour),
		IdleTTLFloor:   getenvDur("IDLE_TTL_FLOOR", 5*time.Minute),
		Seed:           getenvBool("SEED_DEMO", true),
	}
	if c.PostgresDSN != "" && c.Backend == "memory" {
		// Presence of a DSN is an explicit opt-in signal.
		c.Backend = "postgres"
	}
	return c
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenvBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
