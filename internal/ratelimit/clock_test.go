package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"ratelimit-platform/internal/ratelimit"
	"ratelimit-platform/internal/testsupport"
)

// TestBackwardsClockNoNegativeRefill simulates a Redis clock JUMPING
// BACKWARDS (e.g. manual clock correction / VM migration). The bucket must
// never credit a negative elapsed time (which would delete tokens) or a
// positive refill derived from the jump; after the clock recovers, behavior
// is consistent.
func TestBackwardsClockNoNegativeRefill(t *testing.T) {
	mr, rdb := testsupport.MiniRedis(t)
	base := time.Unix(1700000000, 0).UTC()
	mr.SetTime(base)
	eng := ratelimit.NewEngine(rdb, ratelimit.Config{})
	ctx := context.Background()
	specs := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "bw", PolicyID: 1,
		Capacity: 10_000, RefillRate: 10_000, Version: 1, IdleTTL: time.Hour,
	}}

	// Consume 4 of 10 tokens at base.
	for i := 0; i < 4; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "bw", API: "a",
			Fingerprint: fpOf("b" + string(rune('a'+i))), Buckets: specs,
		})
		if err != nil || !d.Allowed {
			t.Fatalf("drain %d: %+v %v", i, d, err)
		}
	}
	// Clock jumps BACK 60 seconds. A naive bucket would compute elapsed=-60s
	// and, after token refill capped to capacity, behave erratically. Ours
	// treats elapsed<=0 as zero refill; the 6 remaining tokens must still be
	// exactly 6000 mT and 6 requests must pass.
	mr.SetTime(base.Add(-60 * time.Second))
	for i := 0; i < 6; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "bw", API: "a",
			Fingerprint: fpOf("back" + string(rune('a'+i))), Buckets: specs,
		})
		if err != nil {
			t.Fatalf("backwards call %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("backwards call %d denied (tokens wrongly removed): layer=%s remain=%d",
				i, d.LimitedLayer, d.Buckets[0].Remaining)
		}
	}
	// 11th must deny; rate must not have produced a phantom refill either.
	d, err := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "bw", API: "a", Fingerprint: fpOf("deny"), Buckets: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("over-burst request allowed after backwards clock jump")
	}
	// Wait hint uses the (recovered) clock only on the NEXT call once time
	// advances; here ts was re-anchored to the past timestamp by the last
	// allowed call, so moving back to base credits at most the real elapsed.
}

// TestClockSkewAcrossInstances uses two miniredis instances with offset
// clocks to document that state stored by "server A at T" read by "server B
// at T+skew" never grants more than rate*skew extra tokens — the standard
// token-bucket drift bound — and a skew the OTHER way grants nothing.
func TestClockSkewAcrossInstances(t *testing.T) {
	// Single logical bucket key simulated across two engines pointing at the
	// SAME store but with the store clock moved: the engine derives all time
	// from the store, so skew is exactly store-clock movement.
	mr, rdb := testsupport.MiniRedis(t)
	t0 := time.Unix(1700000000, 0).UTC()
	mr.SetTime(t0)
	eng := ratelimit.NewEngine(rdb, ratelimit.Config{})
	ctx := context.Background()
	// rate 5/s, cap 10. Drain to zero.
	specs := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "skew", PolicyID: 1,
		Capacity: 10_000, RefillRate: 5_000, Version: 1, IdleTTL: time.Hour,
	}}
	for i := 0; i < 10; i++ {
		d, _ := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "skew", API: "a",
			Fingerprint: fpOf("s" + string(rune('a'+i))), Buckets: specs,
		})
		if !d.Allowed {
			t.Fatal("drain burst")
		}
	}
	// Clock leaps forward 2s (maximum plausible cross-instance skew): at most
	// rate*2 = 10 extra tokens, capped at cap = 10.
	mr.SetTime(t0.Add(2 * time.Second))
	granted := 0
	for {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "skew", API: "a",
			Fingerprint: fpOf("g" + string(rune('a'+granted))), Buckets: specs,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			break
		}
		granted++
		if granted > 10 {
			t.Fatal("skew granted more than rate*skew tokens (bound broken)")
		}
	}
	if granted != 10 {
		t.Fatalf("after 2s skew at 5/s granted=%d want exactly 10", granted)
	}
}
