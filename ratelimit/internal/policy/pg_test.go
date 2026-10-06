package policy

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
)

// PostgreSQL 集成测试：设置 DATABASE_URL 后运行，否则跳过。
// 例：DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/ratelimit?sslmode=disable" go test ./internal/policy/
func TestPgStoreRoundTrip(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM policies`); err != nil {
		t.Fatal(err)
	}

	store := NewPgStore(db)
	if err := SeedDefaults(ctx, store); err != nil {
		t.Fatal(err)
	}
	// 再播一次种子应幂等（不报错、不重复）
	if err := SeedDefaults(ctx, store); err != nil {
		t.Fatal(err)
	}
	list, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("seeded %d policies, want 3", len(list))
	}

	p, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "t1", Capacity: 42, RefillPerSec: 7, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "t1", Capacity: 1, RefillPerSec: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate: %v, want ErrConflict", err)
	}

	p.Capacity = 100
	p.FailOpen = true
	updated, err := store.Update(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Capacity != 100 || !updated.FailOpen {
		t.Fatalf("update: %+v", updated)
	}

	got, err := store.Get(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Capacity != 100 {
		t.Fatalf("get: %+v", got)
	}

	if err := store.Delete(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, p.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v, want ErrNotFound", err)
	}
}
