package policy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResolvePrecedence(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	seed := []Policy{
		{Scope: ScopeUser, ScopeKey: "*", Capacity: 100, RefillPerSec: 10, Enabled: true},
		{Scope: ScopeUser, ScopeKey: "t1:*", Capacity: 50, RefillPerSec: 5, Enabled: true},
		{Scope: ScopeUser, ScopeKey: "t1:u1", Capacity: 5, RefillPerSec: 1, Enabled: true},
	}
	for _, p := range seed {
		if _, err := store.Create(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	r := NewResolver(store, time.Minute)

	cases := []struct {
		tenant, user string
		wantCap      float64
	}{
		{"t1", "u1", 5},   // 精确匹配优先
		{"t1", "u2", 50},  // 租户通配次之
		{"t2", "u1", 100}, // 全局兜底
	}
	for _, tc := range cases {
		p, ok, err := r.Resolve(ctx, ScopeUser, UserKeys(tc.tenant, tc.user)...)
		if err != nil || !ok {
			t.Fatalf("resolve %s/%s: ok=%v err=%v", tc.tenant, tc.user, ok, err)
		}
		if p.Capacity != tc.wantCap {
			t.Fatalf("%s/%s capacity = %v, want %v", tc.tenant, tc.user, p.Capacity, tc.wantCap)
		}
	}

	// 未配置 → 不限制
	if _, ok, _ := r.Resolve(ctx, ScopeTenant, TenantKeys("nope")...); ok {
		t.Fatal("unexpected policy for unconfigured tenant")
	}
}

// 停用的精确策略不应命中，应回退到下一优先级。
func TestResolveSkipsDisabled(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	if _, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "t1", Capacity: 1, RefillPerSec: 0, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "*", Capacity: 100, RefillPerSec: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	r := NewResolver(store, time.Minute)
	p, ok, err := r.Resolve(ctx, ScopeTenant, TenantKeys("t1")...)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if p.Capacity != 100 {
		t.Fatalf("capacity = %v, want fallback to wildcard 100", p.Capacity)
	}
}

func TestMemStoreCRUD(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	p, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "t1", Capacity: 10, RefillPerSec: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if p.ID == 0 || p.UpdatedAt.IsZero() {
		t.Fatalf("bad created policy: %+v", p)
	}
	if _, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "t1", Capacity: 5, RefillPerSec: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate create: %v, want ErrConflict", err)
	}

	p.Capacity = 20
	updated, err := store.Update(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Capacity != 20 {
		t.Fatalf("capacity = %v, want 20", updated.Capacity)
	}
	// scope/scope_key 不可变
	updated.ScopeKey = "t2"
	if _, err := store.Update(ctx, updated); err == nil {
		t.Fatal("expected error changing scope_key")
	}

	if err := store.Delete(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, p.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v, want ErrNotFound", err)
	}
}

// 存储故障时解析器回退到最后一次成功的缓存。
func TestResolverFallsBackToStaleCache(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	if _, err := store.Create(ctx, Policy{Scope: ScopeTenant, ScopeKey: "*", Capacity: 7, RefillPerSec: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	r := NewResolver(store, time.Nanosecond) // 缓存立即过期，每次都回源

	if _, ok, err := r.Resolve(ctx, ScopeTenant, TenantKeys("t1")...); err != nil || !ok {
		t.Fatalf("first resolve: ok=%v err=%v", ok, err)
	}

	// 删除底层数据并模拟存储故障：用一个总是失败的 Store 包一层
	r.store = &failingStore{}
	p, ok, err := r.Resolve(ctx, ScopeTenant, TenantKeys("t1")...)
	if err != nil {
		t.Fatalf("should fall back to stale cache, got err %v", err)
	}
	if !ok || p.Capacity != 7 {
		t.Fatalf("stale fallback: %+v ok=%v", p, ok)
	}
}

type failingStore struct{ Store }

func (f *failingStore) List(context.Context) ([]Policy, error) {
	return nil, errors.New("store down")
}
