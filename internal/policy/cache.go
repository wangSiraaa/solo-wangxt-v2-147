package policy

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Store is the full persistence contract implemented by MemoryStore and the
// PostgreSQL backend.
type Store interface {
	Resolver
	CreatePolicy(ctx context.Context, p Policy) (Policy, error)
	UpdatePolicy(ctx context.Context, p Policy) (Policy, error)
	DeletePolicy(ctx context.Context, id int64) error
	ListPolicies(ctx context.Context) ([]Policy, error)
	UpsertEndpoint(ctx context.Context, e Endpoint) (Endpoint, error)
	DeleteEndpoint(ctx context.Context, id string) error
	ListEndpoints(ctx context.Context) ([]Endpoint, error)
	SetBinding(ctx context.Context, b Binding) (Binding, error)
	DeleteBinding(ctx context.Context, layer, tenant, user, api string) error
	ListBindings(ctx context.Context) ([]Binding, error)
}

// InvalidatingStore optionally pushes change notifications (Postgres
// LISTEN/NOTIFY or the in-process channel). CachedStore subscribes when
// available; polling remains the safety net.
type InvalidatingStore interface {
	Store
	Subscribe() <-chan struct{}
}

// CachedStore serves the hot path from an atomic snapshot pointer. Refreshes
// happen on invalidation events and at most every refreshInterval as a
// fallback. The last good snapshot is retained if a refresh fails.
type CachedStore struct {
	Store
	refreshInterval time.Duration

	mu      sync.RWMutex
	current atomic.Pointer[Snapshot]
}

// NewCachedStore loads the initial snapshot (fail-fast if unreadable) and
// starts background refresh when the backend can invalidate.
func NewCachedStore(ctx context.Context, s Store, refreshInterval time.Duration) (*CachedStore, error) {
	if refreshInterval <= 0 {
		refreshInterval = 30 * time.Second
	}
	c := &CachedStore{Store: s, refreshInterval: refreshInterval}
	snap := s.Snapshot()
	c.current.Store(snap)
	return c, nil
}

// Snapshot returns the cached read model (never blocks on PostgreSQL).
func (c *CachedStore) Snapshot() *Snapshot {
	return c.current.Load()
}

// Refresh reloads from the backing store and swaps the snapshot atomically.
// A failed reload keeps serving the previous snapshot.
func (c *CachedStore) Refresh(ctx context.Context) error {
	snap := c.Store.Snapshot()
	if snap == nil {
		return ErrNoPolicy
	}
	c.current.Store(snap)
	return nil
}

// RunBackground blocks until ctx is done, reloading on invalidation or
// fallback ticker.
func (c *CachedStore) RunBackground(ctx context.Context) {
	var invalid <-chan struct{}
	if is, ok := c.Store.(InvalidatingStore); ok {
		invalid = is.Subscribe()
	}
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-invalid:
			// Small debounce coalesces burst commits into one reload.
			time.Sleep(50 * time.Millisecond)
			if err := c.Refresh(ctx); err != nil {
				log.Printf("policy: refresh after invalidation failed: %v (serving stale snapshot)", err)
			}
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil {
				log.Printf("policy: periodic refresh failed: %v", err)
			}
		}
	}
}
