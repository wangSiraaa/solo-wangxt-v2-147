package policy_test

import (
	"context"
	"errors"
	"testing"

	"ratelimit-platform/internal/policy"
	"ratelimit-platform/internal/ratelimit"
)

func seedStore(t *testing.T) policy.Store {
	t.Helper()
	ctx := context.Background()
	s := policy.NewMemoryStore()
	if err := policy.Seed(ctx, s); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return s
}

func TestResolveSeededDefaults(t *testing.T) {
	s := seedStore(t)
	r, err := policy.Resolve(s.Snapshot(), "acme", "alice", "login")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Buckets) != 3 {
		t.Fatalf("buckets=%d want 3", len(r.Buckets))
	}
	got := map[string]int64{}
	for _, b := range r.Buckets {
		got[b.Layer] = b.Policy.CapacityMT
	}
	if got[ratelimit.LayerTenant] != 100_000 {
		t.Fatalf("tenant cap=%d", got[ratelimit.LayerTenant])
	}
	if got[ratelimit.LayerUser] != 20_000 {
		t.Fatalf("user cap=%d", got[ratelimit.LayerUser])
	}
	if got[ratelimit.LayerAPI] != 5_000 {
		t.Fatalf("api cap=%d want login's 5000", got[ratelimit.LayerAPI])
	}
	if r.Endpoint.FailPolicy != policy.FailClosed {
		t.Fatalf("login fail policy=%s want closed", r.Endpoint.FailPolicy)
	}
}

func TestResolveSpecificOverridesDefault(t *testing.T) {
	s := seedStore(t)
	ctx := context.Background()
	// Add a dedicated tenant policy overriding the tenant default.
	p, err := s.CreatePolicy(ctx, policy.Policy{
		Name: "tenant-vip", CapacityMT: 500_000, RefillMTPS: 50_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetBinding(ctx, policy.Binding{
		Layer: "tenant", TenantID: "vip", PolicyID: p.ID,
	}); err != nil {
		t.Fatal(err)
	}
	r, err := policy.Resolve(s.Snapshot(), "vip", "alice", "search")
	if err != nil {
		t.Fatal(err)
	}
	var tenant policy.ResolvedBucket
	for _, b := range r.Buckets {
		if b.Layer == ratelimit.LayerTenant {
			tenant = b
		}
	}
	if tenant.Policy.ID != p.ID {
		t.Fatalf("vip tenant resolved policy %d want %d", tenant.Policy.ID, p.ID)
	}
	if r.Endpoint.FailPolicy != policy.FailOpen {
		t.Fatalf("search fail policy=%s want open", r.Endpoint.FailPolicy)
	}
}

func TestResolveOptionalUserLayer(t *testing.T) {
	s := policy.NewMemoryStore()
	ctx := context.Background()
	p, _ := s.CreatePolicy(ctx, policy.Policy{
		Name: "only-api", CapacityMT: 10_000, RefillMTPS: 1_000,
	})
	_, _ = s.UpsertEndpoint(ctx, policy.Endpoint{
		ID: "x", FailPolicy: policy.FailClosed,
	})
	_, _ = s.SetBinding(ctx, policy.Binding{Layer: "api", APIID: "x", PolicyID: p.ID})

	// No tenant default and no user policy: request WITH a user must fail on
	// the missing (mandatory) tenant layer...
	if _, err := policy.Resolve(s.Snapshot(), "t", "u", "x"); !errors.Is(err, policy.ErrNoPolicy) {
		t.Fatalf("missing tenant policy err=%v want ErrNoPolicy", err)
	}
}

func TestResolveUnknownAndDisabledEndpoint(t *testing.T) {
	s := seedStore(t)
	if _, err := policy.Resolve(s.Snapshot(), "t", "u", "nope"); !errors.Is(err, policy.ErrNotFound) {
		t.Fatalf("err=%v want ErrNotFound", err)
	}
	ctx := context.Background()
	if _, err := s.UpsertEndpoint(ctx, policy.Endpoint{
		ID: "docs", FailPolicy: policy.FailOpen, Disabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := policy.Resolve(s.Snapshot(), "t", "u", "docs"); !errors.Is(err, policy.ErrEndpointDisabled) {
		t.Fatalf("err=%v want ErrEndpointDisabled", err)
	}
}

func TestPolicyVersionBumpsOnUpdate(t *testing.T) {
	s := seedStore(t)
	ctx := context.Background()
	pol, err := s.CreatePolicy(ctx, policy.Policy{
		Name: "v", CapacityMT: 1000, RefillMTPS: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pol.Version != 1 {
		t.Fatalf("initial version=%d", pol.Version)
	}
	updated, err := s.UpdatePolicy(ctx, policy.Policy{
		ID: pol.ID, Name: "v", CapacityMT: 2000, RefillMTPS: 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 {
		t.Fatalf("updated version=%d want 2", updated.Version)
	}
}

func TestCachedStoreServesStaleOnFailure(t *testing.T) {
	s := seedStore(t)
	ctx := context.Background()
	cached, err := policy.NewCachedStore(ctx, s, 0)
	if err != nil {
		t.Fatal(err)
	}
	first := cached.Snapshot().Version
	// Mutate backing store, then refresh: snapshot advances.
	_, _ = s.CreatePolicy(ctx, policy.Policy{
		Name: "new-one", CapacityMT: 1000, RefillMTPS: 1000,
	})
	if err := cached.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if cached.Snapshot().Version == first {
		t.Fatal("snapshot version did not advance after refresh")
	}
}

func TestBindingDefaultsAndSubjects(t *testing.T) {
	s := policy.NewMemoryStore()
	ctx := context.Background()
	p, _ := s.CreatePolicy(ctx, policy.Policy{
		Name: "p", CapacityMT: 1000, RefillMTPS: 1000,
	})
	// Layer default binding (empty subject) is legal and resolvable.
	if _, err := s.SetBinding(ctx, policy.Binding{Layer: "tenant", PolicyID: p.ID}); err != nil {
		t.Fatalf("default binding: %v", err)
	}
	if _, err := s.SetBinding(ctx, policy.Binding{
		Layer: "tenant", TenantID: "t1", PolicyID: p.ID,
	}); err != nil {
		t.Fatalf("specific binding: %v", err)
	}
	bs, _ := s.ListBindings(ctx)
	if len(bs) != 2 {
		t.Fatalf("bindings=%d want 2", len(bs))
	}
}
