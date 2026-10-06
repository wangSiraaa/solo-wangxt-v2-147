package ratelimit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"ratelimit-platform/internal/ratelimit"
)

// TestRealRedisPolicyHotSwap: tighten then loosen a live bucket via a
// version bump and verify behavior changes without flushing counters.
func TestRealRedisPolicyHotSwap(t *testing.T) {
	eng := realEngine(t, ratelimit.Config{})
	ctx := context.Background()

	v1 := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "hot", PolicyID: 1,
		Capacity: 100_000, RefillRate: 100_000, Version: 1, IdleTTL: time.Hour,
	}}
	// Consume 50 against the loose policy.
	for i := 0; i < 50; i++ {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "hot", API: "a",
			Fingerprint: fpOf(fmt.Sprintf("v1-%d", i)), Buckets: v1,
		})
		if err != nil || !d.Allowed {
			t.Fatalf("v1 %d: %+v %v", i, d, err)
		}
	}

	// Tighten to cap 10, version 2: 50 stored tokens clamp to 10.
	v2 := []ratelimit.BucketSpec{{
		Layer: ratelimit.LayerTenant, Name: "hot", PolicyID: 2,
		Capacity: 10_000, RefillRate: 1000, Version: 2, IdleTTL: time.Hour,
	}}
	granted := 0
	for {
		d, err := eng.Decide(ctx, ratelimit.CheckRequest{
			Tenant: "hot", API: "a",
			Fingerprint: fpOf(fmt.Sprintf("v2-%d", granted)), Buckets: v2,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			break
		}
		granted++
		if granted > 10 {
			t.Fatal("tightened policy admitted over the new burst")
		}
	}
	if granted != 10 {
		t.Fatalf("after clamp to 10 tokens admitted=%d want 10", granted)
	}

	// Next token under the NEW rate (1/s) takes ~1s.
	d, _ := eng.Decide(ctx, ratelimit.CheckRequest{
		Tenant: "hot", API: "a", Fingerprint: fpOf("v2-wait"), Buckets: v2,
	})
	if d.Allowed || d.Wait < 900*time.Millisecond || d.Wait > 1150*time.Millisecond {
		t.Fatalf("post-change wait=%s want ~1s under new rate", d.Wait)
	}
}
