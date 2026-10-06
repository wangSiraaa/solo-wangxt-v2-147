package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T) (*miniredis.Miniredis, *redis.Client, *Limiter) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return mr, rdb, New(rdb)
}

func bucketTokens(t *testing.T, rdb *redis.Client, key string) float64 {
	t.Helper()
	v, err := rdb.HGet(context.Background(), key, "tokens").Float64()
	if err != nil {
		t.Fatalf("read tokens of %s: %v", key, err)
	}
	return v
}

// 基本语义：容量即突发上限，打空后拒绝并给出层级与重试时间。
func TestBasicAllowThenDeny(t *testing.T) {
	_, _, lim := setup(t)
	ctx := context.Background()
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 3, RatePerSec: 1}

	approx := func(got, want float64) bool { return got > want-0.05 && got < want+0.05 }

	for i := 0; i < 3; i++ {
		d, err := lim.Check(ctx, []Bucket{b}, 1, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
		if got, want := d.Remaining[LayerTenant], float64(2-i); !approx(got, want) {
			t.Fatalf("remaining = %v, want ~%v", got, want)
		}
	}

	d, err := lim.Check(ctx, []Bucket{b}, 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("4th request should be denied")
	}
	if d.LimitedBy != LayerTenant {
		t.Fatalf("limited_by = %q, want tenant", d.LimitedBy)
	}
	// 需要 1 个令牌、速率 1/s：等待约 1s
	if d.RetryAfter <= 0 || d.RetryAfter > 1100*time.Millisecond {
		t.Fatalf("retry_after = %v, want in (0, 1.1s]", d.RetryAfter)
	}
}

// 补充公式可核对：tokens = min(capacity, tokens + Δt*rate)，且不超过容量。
// 通过预置历史 ts 构造确定的 Δt（时钟源为 Redis TIME，测试直接操纵桶状态）。
func TestRefillFormula(t *testing.T) {
	_, rdb, lim := setup(t)
	ctx := context.Background()
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 10, RatePerSec: 10}

	// 3 个令牌、ts 在 2 秒前 → 补充 min(10, 3+2*10) = 10（封顶）
	now, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, b.Key, "tokens", 3, "ts", now.Add(-2*time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	d, err := lim.Check(ctx, []Bucket{b}, 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed {
		t.Fatal("should be allowed after refill")
	}
	if got := d.Remaining[LayerTenant]; got < 8.9 || got > 9.1 {
		t.Fatalf("remaining = %v, want ~9 (refill capped at capacity 10)", got)
	}

	// 0 令牌、ts 在 500ms 前、rate 10/s → 补充 ~5 个
	now, err = rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, b.Key, "tokens", 0, "ts", now.Add(-500*time.Millisecond).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	d, err = lim.Check(ctx, []Bucket{b}, 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed {
		t.Fatal("should be allowed after partial refill")
	}
	if got := d.Remaining[LayerTenant]; got < 3.8 || got > 4.2 {
		t.Fatalf("remaining = %v, want ~4", got)
	}
}

// 全有或全无：任一桶不足时，其它桶一个令牌都不能少。
func TestAllOrNothingNoPartialConsumption(t *testing.T) {
	_, rdb, lim := setup(t)
	ctx := context.Background()
	buckets := []Bucket{
		{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 1, RatePerSec: 0},
		{Layer: LayerUser, Key: "rl:{t1}:u:u1", Capacity: 5, RatePerSec: 0},
		{Layer: LayerEndpoint, Key: "rl:{t1}:e:/pay", Capacity: 5, RatePerSec: 0},
	}

	d, err := lim.Check(ctx, buckets, 1, "", 0)
	if err != nil || !d.Allowed {
		t.Fatalf("first request: %+v err=%v", d, err)
	}
	// 第一次后：tenant=0, user=4, endpoint=4

	for i := 0; i < 3; i++ {
		d, err := lim.Check(ctx, buckets, 1, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			t.Fatalf("request %d should be denied by tenant", i+2)
		}
		if d.LimitedBy != LayerTenant {
			t.Fatalf("limited_by = %q, want tenant", d.LimitedBy)
		}
	}
	// 拒绝不得消耗其它桶
	if got := bucketTokens(t, rdb, "rl:{t1}:u:u1"); got != 4 {
		t.Fatalf("user tokens = %v, want 4 (denial must not consume)", got)
	}
	if got := bucketTokens(t, rdb, "rl:{t1}:e:/pay"); got != 4 {
		t.Fatalf("endpoint tokens = %v, want 4 (denial must not consume)", got)
	}
	if got := bucketTokens(t, rdb, "rl:{t1}:t"); got != 0 {
		t.Fatalf("tenant tokens = %v, want 0", got)
	}
}

// 判定幂等：窗口内同一键重试不重复扣减；窗口过期后按新判定处理（缓存有界）。
func TestIdempotencyReplayAndExpiry(t *testing.T) {
	mr, rdb, lim := setup(t)
	ctx := context.Background()
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 10, RatePerSec: 0}

	d1, err := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:k1", 3*time.Second)
	if err != nil || !d1.Allowed || d1.Replay {
		t.Fatalf("first: %+v err=%v", d1, err)
	}
	d2, err := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:k1", 3*time.Second)
	if err != nil || !d2.Allowed || !d2.Replay {
		t.Fatalf("replay: %+v err=%v", d2, err)
	}
	if got := bucketTokens(t, rdb, b.Key); got != 9 {
		t.Fatalf("tokens = %v, want 9: replay must not consume twice", got)
	}

	// 不同键 = 新判定，正常扣减
	if d, _ := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:k2", 3*time.Second); !d.Allowed || d.Replay {
		t.Fatalf("new key should be a fresh decision: %+v", d)
	}
	if got := bucketTokens(t, rdb, b.Key); got != 8 {
		t.Fatalf("tokens = %v, want 8", got)
	}

	// 窗口过期后，同一键按新判定处理（幂等缓存有界，不无限保留）
	mr.FastForward(4 * time.Second)
	d3, err := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:k1", 3*time.Second)
	if err != nil || !d3.Allowed || d3.Replay {
		t.Fatalf("after window expiry: %+v err=%v", d3, err)
	}
	if got := bucketTokens(t, rdb, b.Key); got != 7 {
		t.Fatalf("tokens = %v, want 7", got)
	}
}

// 拒绝的幂等缓存 TTL 不超过 retry_after，避免掩盖已恢复的配额。
func TestDenialCachedOnlyUntilRetryAfter(t *testing.T) {
	mr, rdb, lim := setup(t)
	ctx := context.Background()
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 1, RatePerSec: 1}

	if d, _ := lim.Check(ctx, []Bucket{b}, 1, "", 0); !d.Allowed {
		t.Fatal("drain failed")
	}
	d, err := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:kd", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("should be denied")
	}

	// 幂等窗口 10s，但 retry_after ≈ 1s：缓存 TTL 必须被钳到 ≈1s
	ttl, err := rdb.PTTL(ctx, "rl:{t1}:idem:kd").Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 1100*time.Millisecond {
		t.Fatalf("denial cache ttl = %v, want in (0, 1.1s]", ttl)
	}

	// 窗口内重试 → 重放同一拒绝，不重新评估
	d2, _ := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:kd", 10*time.Second)
	if d2.Allowed || !d2.Replay {
		t.Fatalf("immediate retry should replay denial: %+v", d2)
	}

	// 缓存过期后同一键必须重新评估（此时桶里已有令牌则放行）
	mr.FastForward(1500 * time.Millisecond)
	if n, _ := rdb.Exists(ctx, "rl:{t1}:idem:kd").Result(); n != 0 {
		t.Fatal("denial cache should have expired")
	}
	if err := rdb.HSet(ctx, b.Key, "tokens", 1).Err(); err != nil {
		t.Fatal(err)
	}
	d3, err := lim.Check(ctx, []Bucket{b}, 1, "rl:{t1}:idem:kd", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !d3.Allowed || d3.Replay {
		t.Fatalf("after cache expiry the same key must be re-evaluated: %+v", d3)
	}
}

// 并发突发：rate=0 时，允许量必须恰好等于容量（原子性的强断言）。
func TestConcurrentBurstExactlyCapacity(t *testing.T) {
	_, _, lim := setup(t)
	ctx := context.Background()
	const capacity = 30
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: capacity, RatePerSec: 0}

	var allowed int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := lim.Check(ctx, []Bucket{b}, 1, "", 0)
			if err == nil && d.Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed != capacity {
		t.Fatalf("allowed = %d, want exactly %d (burst must equal capacity)", allowed, capacity)
	}
}

// 并发压测上界核对：允许消耗总量不得超过 capacity + rate*elapsed（加一份 cost 余量），
// 且桶余量永不为负 —— 核对上界，而不是只看平均 QPS。
func TestConcurrentUpperBound(t *testing.T) {
	cases := []struct {
		name     string
		capacity float64
		rate     float64
		cost     float64
		duration time.Duration
	}{
		{"cost1", 50, 20, 1, 1500 * time.Millisecond},
		{"cost3", 100, 40, 3, 1200 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rdb, lim := setup(t)
			ctx := context.Background()
			b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: tc.capacity, RatePerSec: tc.rate}

			var consumed int64 // 允许次数 * cost
			var wg sync.WaitGroup
			start := time.Now()
			stop := start.Add(tc.duration)
			for g := 0; g < 32; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Now().Before(stop) {
						d, err := lim.Check(ctx, []Bucket{b}, tc.cost, "", 0)
						if err != nil {
							t.Errorf("check: %v", err)
							return
						}
						if d.Allowed {
							atomic.AddInt64(&consumed, int64(tc.cost))
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start).Seconds()

			upper := tc.capacity + tc.rate*elapsed + tc.cost // +cost 余量覆盖在途请求
			if float64(consumed) > upper {
				t.Fatalf("consumed %d exceeds upper bound %.2f (capacity %v + rate %v * %.2fs)",
					consumed, upper, tc.capacity, tc.rate, elapsed)
			}
			if tok := bucketTokens(t, rdb, b.Key); tok < 0 {
				t.Fatalf("bucket tokens negative: %v", tok)
			}
			// 下界 sanity：持续高压下实际通过量应接近上界而非远低于它
			lower := tc.capacity + tc.rate*(elapsed-0.5)
			if float64(consumed) < lower {
				t.Fatalf("consumed %d suspiciously low (< %.2f)", consumed, lower)
			}
			t.Logf("consumed=%d upper=%.1f elapsed=%.2fs", consumed, upper, elapsed)
		})
	}
}

// 时钟偏差（未来）：上一个写入者时钟快 30s 时，不得产生额外令牌、不得超过容量。
func TestClockSkewFutureTimestamp(t *testing.T) {
	_, rdb, lim := setup(t)
	ctx := context.Background()
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 5, RatePerSec: 100}

	// 模拟时钟更快的节点写入的状态：ts 在未来 30s
	now, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, b.Key, "tokens", 5, "ts", now.Add(30*time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}

	allowed := 0
	for i := 0; i < 10; i++ {
		d, err := lim.Check(ctx, []Bucket{b}, 1, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed = %d, want 5: future ts must not create extra tokens", allowed)
	}
	if tok := bucketTokens(t, rdb, b.Key); tok > 5 {
		t.Fatalf("tokens = %v exceeds capacity", tok)
	}
}

// 时钟偏差（过去）：ts 在 1 小时前，补充必须被封顶在容量，不能超发。
func TestClockSkewPastTimestampCappedAtCapacity(t *testing.T) {
	_, rdb, lim := setup(t)
	ctx := context.Background()
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 5, RatePerSec: 100}

	now, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, b.Key, "tokens", 0, "ts", now.Add(-time.Hour).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}

	allowed := 0
	for i := 0; i < 10; i++ {
		if d, err := lim.Check(ctx, []Bucket{b}, 1, "", 0); err != nil {
			t.Fatal(err)
		} else if d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed = %d, want 5: refill must be capped at capacity", allowed)
	}
	if tok := bucketTokens(t, rdb, b.Key); tok < 0 || tok > 5 {
		t.Fatalf("tokens = %v out of [0,5]", tok)
	}
}

// Redis 故障时错误向上传递（由 API 层按接口风险配置决定开放/拒绝）。
func TestRedisErrorPropagates(t *testing.T) {
	mr, _, lim := setup(t)
	b := Bucket{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 5, RatePerSec: 1}
	mr.Close()
	if _, err := lim.Check(context.Background(), []Bucket{b}, 1, "", 0); err == nil {
		t.Fatal("expected error when redis is down")
	}
}

// 多桶时 limited_by 应指向约束最强的桶（等待时间最长者）。
func TestLimitedByStrongestConstraint(t *testing.T) {
	_, _, lim := setup(t)
	ctx := context.Background()
	buckets := []Bucket{
		{Layer: LayerTenant, Key: "rl:{t1}:t", Capacity: 100, RatePerSec: 100},
		{Layer: LayerUser, Key: "rl:{t1}:u:u1", Capacity: 1, RatePerSec: 0.1}, // 等 10s
		{Layer: LayerEndpoint, Key: "rl:{t1}:e:/pay", Capacity: 100, RatePerSec: 100},
	}
	if d, _ := lim.Check(ctx, buckets, 1, "", 0); !d.Allowed {
		t.Fatal("first request should pass")
	}
	d, err := lim.Check(ctx, buckets, 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.LimitedBy != LayerUser {
		t.Fatalf("decision = %+v, want denied by user", d)
	}
	if d.RetryAfter < 9*time.Second || d.RetryAfter > 11*time.Second {
		t.Fatalf("retry_after = %v, want ~10s", d.RetryAfter)
	}
}
