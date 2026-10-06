// Package policy 定义限流策略模型、存储接口与带缓存的解析器。
package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Scope 是策略作用的层级。
type Scope string

const (
	ScopeTenant   Scope = "tenant"
	ScopeUser     Scope = "user"
	ScopeEndpoint Scope = "endpoint"
)

// Policy 描述某一层级一个键的令牌桶参数。
type Policy struct {
	ID           int64     `json:"id"`
	Scope        Scope     `json:"scope"`
	ScopeKey     string    `json:"scope_key"` // 精确键，或 "*" / "tenant:*" 通配
	Capacity     float64   `json:"capacity"`
	RefillPerSec float64   `json:"refill_per_sec"`
	FailOpen     bool      `json:"fail_open"` // 仅 endpoint 层级有意义：Redis 故障时放行还是拒绝
	Enabled      bool      `json:"enabled"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (p Policy) Validate() error {
	switch p.Scope {
	case ScopeTenant, ScopeUser, ScopeEndpoint:
	default:
		return fmt.Errorf("invalid scope %q", p.Scope)
	}
	if p.ScopeKey == "" {
		return errors.New("scope_key is required")
	}
	if p.Capacity <= 0 {
		return errors.New("capacity must be > 0")
	}
	if p.RefillPerSec < 0 {
		return errors.New("refill_per_sec must be >= 0")
	}
	return nil
}

var (
	ErrNotFound = errors.New("policy not found")
	ErrConflict = errors.New("policy already exists for scope+scope_key")
)

// Store 是策略的持久化接口（PostgreSQL 实现见 pg.go，MemStore 用于测试）。
type Store interface {
	Create(ctx context.Context, p Policy) (Policy, error)
	Get(ctx context.Context, id int64) (Policy, error)
	Update(ctx context.Context, p Policy) (Policy, error)
	Delete(ctx context.Context, id int64) error
	List(ctx context.Context) ([]Policy, error)
}

func keyOf(s Scope, k string) string { return string(s) + "\x00" + k }

// MemStore 是 Store 的内存实现，供测试与本地开发使用。
type MemStore struct {
	mu     sync.RWMutex
	nextID int64
	byID   map[int64]Policy
	byKey  map[string]int64
}

func NewMemStore() *MemStore {
	return &MemStore{byID: map[int64]Policy{}, byKey: map[string]int64{}}
}

func (m *MemStore) Create(_ context.Context, p Policy) (Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byKey[keyOf(p.Scope, p.ScopeKey)]; ok {
		return Policy{}, ErrConflict
	}
	m.nextID++
	p.ID = m.nextID
	p.UpdatedAt = time.Now()
	m.byID[p.ID] = p
	m.byKey[keyOf(p.Scope, p.ScopeKey)] = p.ID
	return p, nil
}

func (m *MemStore) Get(_ context.Context, id int64) (Policy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.byID[id]
	if !ok {
		return Policy{}, ErrNotFound
	}
	return p, nil
}

func (m *MemStore) Update(_ context.Context, p Policy) (Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.byID[p.ID]
	if !ok {
		return Policy{}, ErrNotFound
	}
	if old.Scope != p.Scope || old.ScopeKey != p.ScopeKey {
		return Policy{}, errors.New("scope and scope_key are immutable")
	}
	p.UpdatedAt = time.Now()
	m.byID[p.ID] = p
	return p, nil
}

func (m *MemStore) Delete(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.byID[id]
	if !ok {
		return ErrNotFound
	}
	delete(m.byID, id)
	delete(m.byKey, keyOf(p.Scope, p.ScopeKey))
	return nil
}

func (m *MemStore) List(_ context.Context) ([]Policy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Policy, 0, len(m.byID))
	for _, p := range m.byID {
		out = append(out, p)
	}
	return out, nil
}

// Resolver 按优先级解析策略，带短 TTL 缓存；写操作后应调用 Invalidate。
// 存储故障时回退到最后一次成功的缓存，避免策略库抖动影响判定链路。
type Resolver struct {
	store Store
	ttl   time.Duration

	mu       sync.RWMutex
	policies map[string]Policy
	loadedAt time.Time
}

func NewResolver(store Store, ttl time.Duration) *Resolver {
	return &Resolver{store: store, ttl: ttl}
}

func (r *Resolver) snapshot(ctx context.Context) (map[string]Policy, error) {
	r.mu.RLock()
	if r.policies != nil && time.Since(r.loadedAt) < r.ttl {
		m := r.policies
		r.mu.RUnlock()
		return m, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.policies != nil && time.Since(r.loadedAt) < r.ttl {
		return r.policies, nil
	}
	list, err := r.store.List(ctx)
	if err != nil {
		if r.policies != nil {
			return r.policies, nil // 存储故障时使用过期缓存
		}
		return nil, err
	}
	m := make(map[string]Policy, len(list))
	for _, p := range list {
		m[keyOf(p.Scope, p.ScopeKey)] = p
	}
	r.policies = m
	r.loadedAt = time.Now()
	return m, nil
}

// Invalidate 使缓存失效，下一次解析重新加载（策略变更立即生效）。
func (r *Resolver) Invalidate() {
	r.mu.Lock()
	r.loadedAt = time.Time{}
	r.mu.Unlock()
}

// Resolve 按 keys 的顺序（最具体在前）返回第一条命中的启用策略。
func (r *Resolver) Resolve(ctx context.Context, scope Scope, keys ...string) (Policy, bool, error) {
	m, err := r.snapshot(ctx)
	if err != nil {
		return Policy{}, false, err
	}
	for _, k := range keys {
		if p, ok := m[keyOf(scope, k)]; ok && p.Enabled {
			return p, true, nil
		}
	}
	return Policy{}, false, nil
}

// TenantKeys、UserKeys、EndpointKeys 给出各层级的解析优先级。
func TenantKeys(tenant string) []string { return []string{tenant, "*"} }

func UserKeys(tenant, user string) []string {
	return []string{tenant + ":" + user, tenant + ":*", "*"}
}

func EndpointKeys(endpoint string) []string { return []string{endpoint, "*"} }
