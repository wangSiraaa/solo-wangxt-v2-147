package ratelimit_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ratelimit-platform/internal/ratelimit"
	"ratelimit-platform/internal/testsupport"
)

// epoch is a fixed instant tests freeze miniredis at so no real wall-clock
// refill leaks between calls; time only advances via mr.FastForward.
var epoch = time.Unix(1700000000, 0).UTC()

func testSpecs() []ratelimit.BucketSpec {
	common := func(layer, name string, c, r int64) ratelimit.BucketSpec {
		return ratelimit.BucketSpec{
			Layer: layer, Name: name, PolicyID: 1,
			Capacity: c, RefillRate: r, Version: 1,
			IdleTTL: time.Minute,
		}
	}
	return []ratelimit.BucketSpec{
		common(ratelimit.LayerTenant, "t1", 5_000, 1_000),
		common(ratelimit.LayerUser, "u1", 5_000, 1_000),
		common(ratelimit.LayerAPI, "a1", 5_000, 1_000),
	}
}

// fpOf is a real, distinct sha256 hex per payload (fingerprint binding is
// part of the contract under test).
func fpOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func newFrozenEngine(t *testing.T) (*frozenEnv, *ratelimit.Engine) {
	t.Helper()
	mr, rdb := testsupport.MiniRedis(t)
	mr.SetTime(epoch)
	eng := ratelimit.NewEngine(rdb, ratelimit.Config{
		IdemAllowTTL: 200 * time.Millisecond,
		IdemDenyTTL:  200 * time.Millisecond,
	})
	return &frozenEnv{mr: mr, now: epoch}, eng
}

type frozenEnv struct {
	mr  miniredisClock
	now time.Time
}

type miniredisClock interface {
	SetTime(t time.Time)
	FastForward(d time.Duration)
}

// advance moves BOTH the TIME clock and TTL timers forward: miniredis's
// FastForward only expires keys; script TIME reads the separately set clock.
func (e *frozenEnv) advance(d time.Duration) {
	e.now = e.now.Add(d)
	e.mr.SetTime(e.now)
	e.mr.FastForward(d)
}

// TestBurstCapacityUpperBound drains fresh buckets: exactly capacity
// requests pass, then the next must be denied. This pins the burst upper
// bound rather than a mean rate.
func TestBurstCapacityUpperBound(t *testing.T) {
	_, eng := newFrozenEngine(t)
	ctx := context.Background()
	specs := testSpecs()

	for i := 0; i < 5; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t1", User: "u1", API: "a1",
			Fingerprint: fpOf("x"), Buckets: specs,
		})
		if err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
		if !d.Allowed {
			t.Fatalf("call %d should be allowed; limited by %s/%s wait=%s",
				i+1, d.LimitedLayer, d.LimitedName, d.Wait)
		}
	}
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", User: "u1", API: "a1",
		Fingerprint: fpOf("x"), Buckets: specs,
	})
	if err != nil {
		t.Fatalf("6th call: %v", err)
	}
	if d.Allowed {
		t.Fatal("6th call must be denied (burst capacity = 5)")
	}
	if d.LimitedLayer != ratelimit.LayerTenant {
		t.Fatalf("limiting layer = %q, want tenant (first evaluated)", d.LimitedLayer)
	}
	// Frozen clock, rate 1 token/s and ~0 tokens left => retry exactly 1s.
	if d.Wait != time.Second {
		t.Fatalf("retry wait = %s, want 1s", d.Wait)
	}
}

// TestDenyConsumesNothing proves the all-or-nothing rule at the storage
// level: after a denial caused by an empty tenant bucket, the user and api
// buckets must retain a full burst (they were never touched).
func TestDenyConsumesNothing(t *testing.T) {
	_, eng := newFrozenEngine(t)
	ctx := context.Background()
	specs := testSpecs()

	// Drain ONLY the tenant bucket (single-bucket decision).
	tenantOnly := []ratelimit.BucketSpec{specs[0]}
	for i := 0; i < 5; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t1", API: "a1",
			Fingerprint: fpOf(fmt.Sprintf("t%d", i)), Buckets: tenantOnly,
		})
		if err != nil || !d.Allowed {
			t.Fatalf("drain tenant %d: %+v err=%v", i, d, err)
		}
	}

	// Multi-bucket decision must deny and must NOT consume user/api.
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", User: "u1", API: "a1",
		Fingerprint: fpOf("m"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("expected denial by tenant bucket")
	}
	if d.LimitedLayer != ratelimit.LayerTenant {
		t.Fatalf("limiting layer=%s", d.LimitedLayer)
	}
	for _, b := range d.Buckets {
		switch b.Layer {
		case ratelimit.LayerTenant:
			if b.Remaining != 0 {
				t.Fatalf("tenant remaining=%d want 0", b.Remaining)
			}
		default:
			if b.Remaining != 5_000 {
				t.Fatalf("bucket %s/%s was consumed on denial: remain=%d want 5000",
					b.Layer, b.Name, b.Remaining)
			}
		}
	}

	// user+api buckets must still serve their full burst (different tenant so
	// tenant bucket is fresh and never limiting).
	userAPI := []ratelimit.BucketSpec{specs[1], specs[2]}
	for i := 0; i < 5; i++ {
		dd, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t2", User: "u1", API: "a1",
			Fingerprint: fpOf(fmt.Sprintf("u%d", i)), Buckets: userAPI,
		})
		if err != nil || !dd.Allowed {
			t.Fatalf("user/api burst call %d: %+v err=%v", i, dd, err)
		}
	}
}

// TestRefillMillisecondPrecision advances the simulated Redis clock and
// checks the floor() refill formula exactly.
func TestRefillMillisecondPrecision(t *testing.T) {
	env, eng := newFrozenEngine(t)
	ctx := context.Background()
	// cap=5000mT, rate=1000mT/s => 1 token/s. Drain, then advance 2350ms.
	specs := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "t1", PolicyID: 1,
		Capacity: 5_000, RefillRate: 1_000, Version: 1, IdleTTL: time.Minute,
	}}
	for i := 0; i < 5; i++ {
		if d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t1", API: "a1",
			Fingerprint: fpOf(fmt.Sprintf("r%d", i)), Buckets: specs,
		}); err != nil || !d.Allowed {
			t.Fatalf("drain %d: %+v %v", i, d, err)
		}
	}
	// Frozen clock: immediate call denied with zero refill.
	d, _ := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", Fingerprint: fpOf("r2"), Buckets: specs,
	})
	if d.Allowed {
		t.Fatal("token with zero elapsed")
	}
	env.advance(2350 * time.Millisecond)
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", Fingerprint: fpOf("r3"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed {
		t.Fatalf("after 2350ms at 1/s should have >=1 token: wait=%s", d.Wait)
	}
	// floor(2350*1000/1000)=2350mT, minus 1000 => 1350mT remaining.
	if got := d.Buckets[0].Remaining; got != 1_350 {
		t.Fatalf("remaining=%d want 1350 mT", got)
	}
}

// TestPolicyChangeClamp verifies a capacity decrease clamps stored tokens and
// an increase makes the new burst reachable.
func TestPolicyChangeClamp(t *testing.T) {
	env, eng := newFrozenEngine(t)
	ctx := context.Background()

	v1 := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "t1", PolicyID: 7,
		Capacity: 10_000, RefillRate: 1_000, Version: 1, IdleTTL: time.Hour,
	}}
	// Consume 3; 7 tokens remain.
	for i := 0; i < 3; i++ {
		d, _ := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t1", API: "a1",
			Fingerprint: fpOf(fmt.Sprintf("p1-%d", i)), Buckets: v1,
		})
		if !d.Allowed {
			t.Fatal("v1 drain")
		}
	}
	// Shrink capacity to 5 tokens with a NEW version: 7 stored -> clamp 5.
	v2 := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "t1", PolicyID: 8,
		Capacity: 5_000, RefillRate: 1_000, Version: 2, IdleTTL: time.Hour,
	}}
	for i := 0; i < 5; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t1", API: "a1",
			Fingerprint: fpOf(fmt.Sprintf("p2-%d", i)), Buckets: v2,
		})
		if err != nil {
			t.Fatalf("v2 call %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("v2 call %d denied after clamp (7 stored -> capped 5): layer=%s",
				i, d.LimitedLayer)
		}
	}
	d, _ := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", Fingerprint: fpOf("p3"), Buckets: v2,
	})
	if d.Allowed {
		t.Fatal("6th token after shrink must deny")
	}

	// Grow capacity to 8, raise refill to 100/s, advance 1s: refill 100
	// capped at 8, then consume 1 => 7000 mT remain.
	v3 := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "t1", PolicyID: 9,
		Capacity: 8_000, RefillRate: 100_000, Version: 3, IdleTTL: time.Hour,
	}}
	env.advance(time.Second)
	d, _ = eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", Fingerprint: fpOf("p4"), Buckets: v3,
	})
	if !d.Allowed || d.Buckets[0].Remaining != 7_000 {
		t.Fatalf("after grow: allowed=%v remain=%d want 7000", d.Allowed,
			d.Buckets[0].Remaining)
	}
}

// TestDecisionIdempotencyWindow checks replay semantics, fingerprint binding
// and bounded TTL expiry for DECISION retries (distinct from business dedup).
func TestDecisionIdempotencyWindow(t *testing.T) {
	env, eng := newFrozenEngine(t)
	ctx := context.Background()
	specs := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "t1", PolicyID: 1,
		Capacity: 1_000, RefillRate: 0, Version: 1, IdleTTL: time.Minute,
	}}
	fp := fpOf("samepayload")

	// First call consumes the single token.
	d1, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", IdempotencyKey: "k-1",
		Fingerprint: fp, Buckets: specs,
	})
	if err != nil || !d1.Allowed {
		t.Fatalf("first: %+v %v", d1, err)
	}

	// Replay returns the original ALLOW without charging again.
	for i := 0; i < 3; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "t1", API: "a1", IdempotencyKey: "k-1",
			Fingerprint: fp, Buckets: specs,
		})
		if err != nil || !d.Allowed || !d.Replayed {
			t.Fatalf("replay %d: %+v err=%v", i, d, err)
		}
	}

	// Same key, different payload -> conflict, stored snapshot not overwritten.
	_, err = eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", IdempotencyKey: "k-1",
		Fingerprint: fpOf("differentpayload"), Buckets: specs,
	})
	if !errors.Is(err, ratelimit.ErrConflict) {
		t.Fatalf("conflict err = %v", err)
	}

	// After the replay window expires the SAME key computes a fresh decision,
	// which must now deny (bucket was drained by exactly one real charge).
	env.advance(250 * time.Millisecond)
	d2, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", IdempotencyKey: "k-1",
		Fingerprint: fp, Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d2.Allowed || d2.Replayed {
		t.Fatalf("fresh decision after window should deny, got %+v", d2)
	}

	// rate=0 => request can never fit.
	if !d2.NeverRetry || d2.LimitedLayer != ratelimit.LayerTenant {
		t.Fatalf("zero-rate bucket: never=%v layer=%s", d2.NeverRetry, d2.LimitedLayer)
	}

	// A denied decision is replayable within its own short window.
	d3, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", IdempotencyKey: "k-1",
		Fingerprint: fp, Buckets: specs,
	})
	if err != nil || d3.Allowed || !d3.Replayed {
		t.Fatalf("deny replay: %+v err=%v", d3, err)
	}
}

// TestBusinessDedupSeparated verifies the independent namespace/window:
// first claim, duplicate, conflict, TTL expiry.
func TestBusinessDedupSeparated(t *testing.T) {
	env, eng := newFrozenEngine(t)
	ctx := context.Background()

	r1, err := eng.BusinessDedup(ctx, "t1", "order-42", fpOf("payloadA"), time.Second)
	if err != nil || !r1.First {
		t.Fatalf("first claim: %+v %v", r1, err)
	}
	r2, err := eng.BusinessDedup(ctx, "t1", "order-42", fpOf("payloadA"), time.Second)
	if err != nil || !r2.Duplicate {
		t.Fatalf("duplicate: %+v %v", r2, err)
	}
	_, err = eng.BusinessDedup(ctx, "t1", "order-42", fpOf("payloadB"), time.Second)
	if !errors.Is(err, ratelimit.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	env.advance(1100 * time.Millisecond)
	r3, err := eng.BusinessDedup(ctx, "t1", "order-42", fpOf("payloadC"), time.Second)
	if err != nil || !r3.First {
		t.Fatalf("after ttl should reclaim: %+v %v", r3, err)
	}
}

// TestCostExceedsCapacity is a static rejection, never reaches Redis state.
func TestCostExceedsCapacity(t *testing.T) {
	_, eng := newFrozenEngine(t)
	specs := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerAPI, Name: "a1", PolicyID: 1,
		Capacity: 500, RefillRate: 1, Version: 1, IdleTTL: time.Minute,
	}}
	_, err := eng.Decide(context.Background(), ratelimit.CheckRequest{
		Tenant: "t1", API: "a1", Cost: 1000,
		Fingerprint: fpOf("c"), Buckets: specs,
	})
	if !errors.Is(err, ratelimit.ErrCostTooLarge) {
		t.Fatalf("err=%v want ErrCostTooLarge", err)
	}
}

// TestConcurrentAllOrNothing hammers one bucket with many goroutines. Exactly
// the burst capacity is admitted (rate is zero, clock frozen); every other
// request is denied and nothing is over-consumed.
func TestConcurrentAllOrNothing(t *testing.T) {
	mr, rdb := testsupport.MiniRedis(t)
	mr.SetTime(epoch)
	eng := ratelimit.NewEngine(rdb, ratelimit.Config{})
	ctx := context.Background()
	const burst = 20
	specs := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "t1", PolicyID: 1,
		Capacity: burst * 1000, RefillRate: 0, Version: 1, IdleTTL: time.Minute,
	}}
	var allowed, denied int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, err := eng.Decide(ctx, ratelimit.CheckRequest{
				Tenant: "t1", API: "a1",
				Fingerprint: fpOf(fmt.Sprintf("z%d", i)), Buckets: specs,
			})
			if err != nil {
				t.Errorf("decide: %v", err)
				return
			}
			if d.Allowed {
				atomic.AddInt64(&allowed, 1)
			} else {
				atomic.AddInt64(&denied, 1)
			}
		}(i)
	}
	wg.Wait()
	if allowed != burst {
		t.Fatalf("allowed=%d want exactly %d (no over-admit); denied=%d",
			allowed, burst, denied)
	}
	if denied != 200-burst {
		t.Fatalf("denied=%d want %d", denied, 200-burst)
	}
}
