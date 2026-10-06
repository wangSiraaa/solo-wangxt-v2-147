package testsupport

import (
	"fmt"
	"os"
	"testing"
	"time"

	embedded "github.com/fergusstrange/embedded-postgres"
)

// EmbeddedPostgres downloads (first run) and starts a real PostgreSQL on an
// ephemeral port, returning a connection DSN. Skipped automatically if the
// binary cannot be fetched (offline) unless RL_REQUIRE_POSTGRES=1.
//
// The downloaded binary is cached under RL_PG_CACHE (or the package default
// in the OS cache dir) so repeated test runs stay fast.
func EmbeddedPostgres(t testing.TB) (dsn string, cleanup func()) {
	t.Helper()
	cacheDir := os.Getenv("RL_PG_CACHE")
	if cacheDir == "" {
		home, err := os.UserCacheDir()
		if err != nil {
			home = t.TempDir()
		}
		cacheDir = home + "/ratelimit-platform-embedded-pg"
		_ = os.MkdirAll(cacheDir, 0o755)
	}
	dataDir := t.TempDir()
	port := uint32(freePort(t))

	cfg := embedded.DefaultConfig().
		Version(embedded.V16).
		Port(port).
		CachePath(cacheDir).
		DataPath(dataDir).
		Username("rl").
		Password("rl").
		Database("rltest").
		StartTimeout(60 * time.Second)

	ep := embedded.NewDatabase(cfg)
	if err := ep.Start(); err != nil {
		if os.Getenv("RL_REQUIRE_POSTGRES") == "1" {
			t.Fatalf("start embedded postgres: %v", err)
		}
		t.Skipf("embedded postgres unavailable (offline?): %v", err)
	}
	dsn = fmt.Sprintf(
		"postgres://rl:rl@127.0.0.1:%d/rltest?sslmode=disable&timezone=UTC", port)

	cleanup = func() { _ = ep.Stop() }
	return dsn, cleanup
}
