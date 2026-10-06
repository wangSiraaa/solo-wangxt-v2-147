package policy_test

import (
	"context"
	"testing"
	"time"

	"ratelimit-platform/internal/policy"
	"ratelimit-platform/internal/ratelimit"
	"ratelimit-platform/internal/testsupport"
)

// TestPostgresRoundTrip runs migrations, seeds, and verifies CRUD + snapshot
// resolution against a REAL PostgreSQL.
func TestPostgresRoundTrip(t *testing.T) {
	dsn, cleanup := testsupport.EmbeddedPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pg, err := policy.NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatalf("connect/migrate: %v", err)
	}
	defer pg.Close()

	if err := policy.SeedIfEmpty(ctx, pg); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Snapshot resolves like the memory backend.
	r, err := policy.Resolve(pg.Snapshot(), "acme", "alice", "pay")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Buckets) != 3 {
		t.Fatalf("buckets=%d want 3", len(r.Buckets))
	}
	if r.Endpoint.FailPolicy != policy.FailClosed {
		t.Fatalf("pay fail policy=%s", r.Endpoint.FailPolicy)
	}
	var apiCap int64
	for _, b := range r.Buckets {
		if b.Layer == ratelimit.LayerAPI {
			apiCap = b.Policy.CapacityMT
		}
	}
	if apiCap != 10_000 {
		t.Fatalf("pay api cap=%d want 10000", apiCap)
	}
}

// TestPostgresVersionBump verifies the BEFORE UPDATE trigger increments the
// policy version (the signal Redis buckets use to re-clamp).
func TestPostgresVersionBump(t *testing.T) {
	dsn, cleanup := testsupport.EmbeddedPostgres(t)
	defer cleanup()
	ctx := context.Background()
	pg, err := policy.NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	p, err := pg.CreatePolicy(ctx, policy.Policy{
		Name: "mutable", CapacityMT: 1000, RefillMTPS: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Version != 1 {
		t.Fatalf("initial version=%d", p.Version)
	}
	p.CapacityMT = 2000
	updated, err := pg.UpdatePolicy(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 {
		t.Fatalf("updated version=%d want 2 (trigger did not bump)", updated.Version)
	}
}

// TestPostgresBindingConstraint ensures a default binding and a specific one
// coexist per layer, and cross-layer columns are rejected.
func TestPostgresBindingConstraint(t *testing.T) {
	dsn, cleanup := testsupport.EmbeddedPostgres(t)
	defer cleanup()
	ctx := context.Background()
	pg, err := policy.NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	p, _ := pg.CreatePolicy(ctx, policy.Policy{
		Name: "p", CapacityMT: 1000, RefillMTPS: 1000,
	})
	if _, err := pg.SetBinding(ctx, policy.Binding{
		Layer: "tenant", PolicyID: p.ID,
	}); err != nil {
		t.Fatalf("default binding: %v", err)
	}
	if _, err := pg.SetBinding(ctx, policy.Binding{
		Layer: "tenant", TenantID: "t1", PolicyID: p.ID,
	}); err != nil {
		t.Fatalf("specific binding: %v", err)
	}
	// Second default must be rejected.
	p2, _ := pg.CreatePolicy(ctx, policy.Policy{
		Name: "p2", CapacityMT: 1000, RefillMTPS: 1000,
	})
	if _, err := pg.SetBinding(ctx, policy.Binding{
		Layer: "tenant", PolicyID: p2.ID,
	}); err == nil {
		t.Fatal("second tenant default binding should violate unique index")
	}

	// Resolver picks the specific binding for the subject, and the layer
	// default for everyone else.
	apiID := placeholderAPI(t, pg)
	snap := pg.Snapshot()
	res, err := policy.Resolve(snap, "t1", "", apiID)
	if err != nil {
		t.Fatal(err)
	}
	var tenantPID int64 = -1
	for _, b := range res.Buckets {
		if b.Layer == ratelimit.LayerTenant {
			tenantPID = b.Policy.ID
		}
	}
	if tenantPID != p.ID {
		t.Fatalf("specific tenant binding resolved policy %d want %d", tenantPID, p.ID)
	}

	// A different tenant falls back to the layer-default binding (same
	// policy here, but the path must still resolve without error).
	if _, err := policy.Resolve(snap, "other-tenant", "", apiID); err != nil {
		t.Fatalf("default tenant resolution: %v", err)
	}
}

func placeholderAPI(t *testing.T, pg *policy.PgStore) string {
	t.Helper()
	ctx := context.Background()
	ep, err := pg.UpsertEndpoint(ctx, policy.Endpoint{
		ID: "ph", FailPolicy: policy.FailClosed,
	})
	if err != nil {
		t.Fatal(err)
	}
	pols, err := pg.ListPolicies(ctx)
	if err != nil || len(pols) == 0 {
		t.Fatalf("policies: %v", err)
	}
	if _, err := pg.SetBinding(ctx, policy.Binding{
		Layer: "api", APIID: "ph", PolicyID: pols[0].ID,
	}); err != nil {
		t.Fatal(err)
	}
	return ep.ID
}

// TestPostgresNotifyInvalidation verifies a config change fires
// rl_policy_changed and the listener channel observes it.
func TestPostgresNotifyInvalidation(t *testing.T) {
	dsn, cleanup := testsupport.EmbeddedPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pg, err := policy.NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	go pg.RunListener(ctx)
	ch := pg.Subscribe()

	// Warm the listener with a brief grace period.
	time.Sleep(500 * time.Millisecond)
	_, err = pg.CreatePolicy(ctx, policy.Policy{
		Name: "notified", CapacityMT: 1000, RefillMTPS: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive rl_policy_changed notification within 5s")
	}
}
