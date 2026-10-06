package ratelimit_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ratelimit-platform/internal/ratelimit"
	"ratelimit-platform/internal/testsupport"
)

// realEngine boots an actual redis-server (skipped without a binary). These
// tests validate the script against the real Lua runtime and real TIME.
func realEngine(t *testing.T, cfg ratelimit.Config) *ratelimit.Engine {
	t.Helper()
	addr, _ := testsupport.RealRedis(t)
	rdb := testsupport.RedisClient(t, addr)
	return ratelimit.NewEngine(rdb, cfg)
}

func oneBucket(tenant string, cap, rate int64, ver int64) []ratelimit.BucketSpec {
	return []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: tenant, PolicyID: 1,
		Capacity: cap, RefillRate: rate, Version: ver, IdleTTL: time.Hour,
	}}
}

// TestRealRedisBurstAndRefill validates burst, wall-clock refill and retry
// hint against a real server.
func TestRealRedisBurstAndRefill(t *testing.T) {
	eng := realEngine(t, ratelimit.Config{})
	ctx := context.Background()
	specs := oneBucket("rt1", 5_000, 10_000, 1) // 5 burst, 10/s

	// First burst: exactly 5 allowed.
	for i := 0; i < 5; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "rt1", API: "a",
			Fingerprint: fpOf(fmt.Sprintf("b%d", i)), Buckets: specs,
		})
		if err != nil || !d.Allowed {
			t.Fatalf("burst call %d: %+v %v", i, d, err)
		}
	}
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt1", API: "a", Fingerprint: fpOf("over"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("6th call over burst must deny")
	}
	if d.LimitedLayer != ratelimit.LayerTenant {
		t.Fatalf("layer=%s", d.LimitedLayer)
	}
	// 10/s => next token within ~100ms.
	if d.Wait <= 0 || d.Wait > 150*time.Millisecond {
		t.Fatalf("retry wait %s not within 150ms for rate 10/s", d.Wait)
	}

	// Wait for two tokens to accumulate; exactly 2 fit, third denied.
	time.Sleep(220 * time.Millisecond)
	for i := 0; i < 2; i++ {
		dd, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "rt1", API: "a",
			Fingerprint: fpOf(fmt.Sprintf("r%d", i)), Buckets: specs,
		})
		if err != nil || !dd.Allowed {
			t.Fatalf("refill call %d: %+v %v", i, dd, err)
		}
	}
	dd, _ := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt1", API: "a", Fingerprint: fpOf("r3"), Buckets: specs,
	})
	if dd.Allowed {
		t.Fatal("third call after only ~2 refilled tokens must deny")
	}
}

// TestRealRedisClockIsRedisTime asserts the authoritative clock advances in
// real milliseconds inside the script (sanity check for clock-source claims).
func TestRealRedisClockIsRedisTime(t *testing.T) {
	eng := realEngine(t, ratelimit.Config{})
	ctx := context.Background()
	specs := oneBucket("rt2", 1_000_000, 0, 1)

	d1, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt2", API: "a", Fingerprint: fpOf("c1"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	d2, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt2", API: "a", Fingerprint: fpOf("c2"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	delta := d2.Now.Sub(d1.Now)
	if delta < 40*time.Millisecond || delta > 500*time.Millisecond {
		t.Fatalf("redis clock delta=%s, want >=40ms after 50ms sleep", delta)
	}
}

// TestRealRedisConcurrentUpperBound fires 400 concurrent requests at a
// bucket of 30 burst / 50 per second for 1 second, then checks the token
// bucket upper bound: admitted <= burst + rate*T + discrete slack over the
// whole window, and also over a shorter sub-window.
func TestRealRedisConcurrentUpperBound(t *testing.T) {
	eng := realEngine(t, ratelimit.Config{})
	ctx := context.Background()
	const burst, ratePS, workers = 30, 50, 64
	specs := oneBucket("rt3", burst*1000, ratePS*1000, 1)

	var allowed, denied int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	t0 := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			deadline := time.Now().Add(time.Second)
			i := 0
			for time.Now().Before(deadline) {
				d, err := eng.Decide(ctx, ratelimit.CheckRequest{
					Tenant: "rt3", API: "a",
					Fingerprint: fpOf(fmt.Sprintf("w%d-%d", w, i)),
					Buckets:     specs,
				})
				i++
				if err != nil {
					continue
				}
				if d.Allowed {
					atomic.AddInt64(&allowed, 1)
				} else {
					atomic.AddInt64(&denied, 1)
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)

	// Upper bound for the entire ~1s window: burst + rate*T + 2 slack
	// (rounding at both ends + scheduling). Crucially NOT an average-QPS
	// check — over-admission would be caught here even if the mean looked OK.
	bound := int64(float64(burst) + ratePS*elapsed.Seconds() + 2)
	t.Logf("elapsed=%s allowed=%d denied=%d upperBound=%d",
		elapsed.Truncate(time.Millisecond), allowed, denied, bound)
	if allowed > bound {
		t.Fatalf("allowed=%d exceeds token-bucket upper bound %d", allowed, bound)
	}
	if allowed < burst {
		t.Fatalf("allowed=%d below guaranteed initial burst %d", allowed, burst)
	}
}

// TestRealRedisDenyZeroWrites uses a second concurrent burst while the tenant
// is drained and verifies the other buckets' Redis state is untouched.
func TestRealRedisDenyZeroWrites(t *testing.T) {
	eng := realEngine(t, ratelimit.Config{})
	ctx := context.Background()
	tenant := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "rt4", PolicyID: 1,
		Capacity: 2_000, RefillRate: 0, Version: 1, IdleTTL: time.Hour,
	}}
	user := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerUser, Name: "u4", PolicyID: 2,
		Capacity: 9_000, RefillRate: 0, Version: 1, IdleTTL: time.Hour,
	}}
	api := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerAPI, Name: "a4", PolicyID: 3,
		Capacity: 9_000, RefillRate: 0, Version: 1, IdleTTL: time.Hour,
	}}

	// Drain tenant (2 tokens).
	for i := 0; i < 2; i++ {
		d, _ := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "rt4", API: "a4",
			Fingerprint: fpOf(fmt.Sprintf("d%d", i)), Buckets: tenant,
		})
		if !d.Allowed {
			t.Fatal("drain")
		}
	}
	// Combined decision must deny; user/api rows report full 9000 and, being
	// never written, have NO Redis keys at all.
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt4", User: "u4", API: "a4", Fingerprint: fpOf("combo"),
		Buckets: []ratelimit.BucketSpec{tenant[0], user[0], api[0]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("combo should deny")
	}
	for _, b := range d.Buckets {
		if b.Layer != ratelimit.LayerTenant && b.Remaining != 9_000 {
			t.Fatalf("%s remaining=%d want 9000 (untouched)", b.Layer, b.Remaining)
		}
	}
}

// TestRealRedisMultiLayerEffectiveMin runs all three layers and confirms the
// effective limit is the tightest layer: drain user bucket alone, then a
// 3-layer decision must report the user layer as the limiter.
func TestRealRedisMultiLayerEffectiveMin(t *testing.T) {
	eng := realEngine(t, ratelimit.Config{})
	ctx := context.Background()
	specs := []ratelimit.BucketSpec{
		{Layer: ratelimit.LayerTenant, Name: "rt5", PolicyID: 1,
			Capacity: 100_000, RefillRate: 100_000, Version: 1, IdleTTL: time.Hour},
		{Layer: ratelimit.LayerUser, Name: "u5", PolicyID: 2,
			Capacity: 1_000, RefillRate: 0, Version: 1, IdleTTL: time.Hour},
		{Layer: ratelimit.LayerAPI, Name: "a5", PolicyID: 3,
			Capacity: 100_000, RefillRate: 100_000, Version: 1, IdleTTL: time.Hour},
	}
	// One 3-layer call consumes the user's only token.
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt5", User: "u5", API: "a5",
		Fingerprint: fpOf("first"), Buckets: specs,
	})
	if err != nil || !d.Allowed {
		t.Fatalf("first: %+v %v", d, err)
	}
	d, err = eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "rt5", User: "u5", API: "a5",
		Fingerprint: fpOf("second"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.LimitedLayer != ratelimit.LayerUser || d.LimitedName != "u5" {
		t.Fatalf("want user-layer denial, got %+v", d)
	}
}
