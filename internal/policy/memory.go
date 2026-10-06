package policy

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
)

// MemoryStore is an in-process Store implementation. It is the default
// backend when PostgreSQL is not configured, and backs unit tests.
type MemoryStore struct {
	mu       sync.RWMutex
	policies map[int64]Policy
	eps      map[string]Endpoint
	bindings map[string]Binding // by bindingKey
	nextPID  int64
	nextBID  int64
	version  atomic.Uint64
	subs     []chan struct{}
}

// NewMemoryStore returns an empty memory store with seeded ids starting at 100.
func NewMemoryStore() *MemoryStore {
	m := &MemoryStore{
		policies: map[int64]Policy{},
		eps:      map[string]Endpoint{},
		bindings: map[string]Binding{},
		nextPID:  100,
		nextBID:  100,
	}
	m.version.Store(1)
	return m
}

// Snapshot returns a deep, immutable copy of the current read model.
func (m *MemoryStore) Snapshot() *Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := &Snapshot{
		Version:   m.version.Load(),
		Policies:  make(map[int64]Policy, len(m.policies)),
		Endpoints: make(map[string]Endpoint, len(m.eps)),
		Bindings:  make(map[string]Binding, len(m.bindings)),
	}
	for k, v := range m.policies {
		snap.Policies[k] = v
	}
	for k, v := range m.eps {
		snap.Endpoints[k] = v
	}
	for k, v := range m.bindings {
		snap.Bindings[k] = v
	}
	return snap
}

// --- policy CRUD -----------------------------------------------------------

// CreatePolicy inserts a new policy and returns it with id/version assigned.
func (m *MemoryStore) CreatePolicy(_ context.Context, p Policy) (Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p.ID = m.nextPID
	m.nextPID++
	p.Version = 1
	m.policies[p.ID] = p
	m.bumpLocked()
	return p, nil
}

// UpdatePolicy overwrites mutable fields and bumps the version so running
// buckets re-clamp to the new capacity on their next decision.
func (m *MemoryStore) UpdatePolicy(_ context.Context, p Policy) (Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.policies[p.ID]
	if !ok {
		return Policy{}, ErrNotFound
	}
	cur.Name = p.Name
	cur.CapacityMT = p.CapacityMT
	cur.RefillMTPS = p.RefillMTPS
	cur.Description = p.Description
	cur.Disabled = p.Disabled
	cur.Version++
	m.policies[p.ID] = cur
	m.bumpLocked()
	return cur, nil
}

// DeletePolicy removes a policy (bindings referencing it fail closed on
// resolution until reassigned).
func (m *MemoryStore) DeletePolicy(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.policies[id]; !ok {
		return ErrNotFound
	}
	delete(m.policies, id)
	for k, b := range m.bindings {
		if b.PolicyID == id {
			delete(m.bindings, k)
		}
	}
	m.bumpLocked()
	return nil
}

// ListPolicies returns policies ordered by id.
func (m *MemoryStore) ListPolicies(_ context.Context) ([]Policy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Policy, 0, len(m.policies))
	for _, p := range m.policies {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// --- endpoint CRUD ---------------------------------------------------------

// UpsertEndpoint creates or replaces an endpoint definition.
func (m *MemoryStore) UpsertEndpoint(_ context.Context, e Endpoint) (Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eps[e.ID] = e
	m.bumpLocked()
	return e, nil
}

// DeleteEndpoint removes an endpoint.
func (m *MemoryStore) DeleteEndpoint(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.eps[id]; !ok {
		return ErrNotFound
	}
	delete(m.eps, id)
	for k, b := range m.bindings {
		if b.Layer == "api" && b.APIID == id {
			delete(m.bindings, k)
		}
	}
	m.bumpLocked()
	return nil
}

// ListEndpoints returns endpoints sorted by id.
func (m *MemoryStore) ListEndpoints(_ context.Context) ([]Endpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Endpoint, 0, len(m.eps))
	for _, e := range m.eps {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// --- bindings --------------------------------------------------------------

// SetBinding creates or replaces the (layer, subject) binding.
func (m *MemoryStore) SetBinding(_ context.Context, b Binding) (Binding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, err := bindingOf(b)
	if err != nil {
		return Binding{}, err
	}
	if existing, ok := m.bindings[key]; ok {
		b.ID = existing.ID
	} else {
		b.ID = m.nextBID
		m.nextBID++
	}
	if p, ok := m.policies[b.PolicyID]; ok {
		b.PolicyName = p.Name
	}
	m.bindings[key] = b
	m.bumpLocked()
	return b, nil
}

// DeleteBinding removes one specific binding.
func (m *MemoryStore) DeleteBinding(_ context.Context, layer, tenant, user, api string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	subject := subjectOf(layer, tenant, user, api)
	key := bindingKey(layer, subject)
	if _, ok := m.bindings[key]; !ok {
		return ErrNotFound
	}
	delete(m.bindings, key)
	m.bumpLocked()
	return nil
}

// ListBindings returns bindings sorted by id.
func (m *MemoryStore) ListBindings(_ context.Context) ([]Binding, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Binding, 0, len(m.bindings))
	for _, b := range m.bindings {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Version reports the current read-model version.
func (m *MemoryStore) Version() uint64 { return m.version.Load() }

// Subscribe returns a channel signaled after each commit (used by the
// cached store instead of PostgreSQL NOTIFY in memory mode).
func (m *MemoryStore) Subscribe() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan struct{}, 1)
	m.subs = append(m.subs, ch)
	return ch
}

func (m *MemoryStore) bumpLocked() {
	m.version.Add(1)
	for _, ch := range m.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func bindingOf(b Binding) (string, error) {
	subject := subjectOf(b.Layer, b.TenantID, b.UserID, b.APIID)
	return bindingKey(b.Layer, subject), nil
}

func subjectOf(layer, tenant, user, api string) string {
	switch layer {
	case "tenant":
		return tenant
	case "user":
		return user
	case "api":
		return api
	}
	return ""
}
