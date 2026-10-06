package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"ratelimit-platform/internal/api"
	"ratelimit-platform/internal/policy"
	"ratelimit-platform/internal/ratelimit"
	"ratelimit-platform/internal/testsupport"
)

type harness struct {
	srv   *httptest.Server
	store policy.Store
	rdb   *redis.Client
	clock *clockHolder
}

type clockHolder struct{ t time.Time }

func newHarness(t *testing.T) *harness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	mr, rdb := testsupport.MiniRedis(t)
	mr.SetTime(time.Unix(1700000000, 0).UTC())

	store := policy.NewMemoryStore()
	if err := policy.Seed(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	cached, err := policy.NewCachedStore(context.Background(), store, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	eng := ratelimit.NewEngine(rdb, ratelimit.Config{
		IdemAllowTTL: 300 * time.Millisecond,
		IdemDenyTTL:  300 * time.Millisecond,
	})
	clk := &clockHolder{t: time.Unix(1700000000, 0).UTC()}
	s := &api.Server{
		Resolver:  cached,
		Engine:    eng,
		Clock:     func() time.Time { return clk.t },
		IdleFloor: time.Minute,
		RedisHealth: func() error {
			return rdb.Ping(context.Background()).Err()
		},
	}
	s.SetRedisTimeFn(func(ctx context.Context) (time.Time, error) {
		return rdb.Time(ctx).Result()
	})

	r := gin.New()
	s.Register(r)
	admin := &api.AdminServer{
		Store:   store,
		Refresh: func() error { return cached.Refresh(context.Background()) },
	}
	admin.RegisterAdmin(r)

	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)
	return &harness{srv: ts, store: store, rdb: rdb, clock: clk}
}

func (h *harness) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestCheckAllowDenyShape(t *testing.T) {
	h := newHarness(t)
	// login policy: burst 5, 0.1/s. Drain 5, then expect a 429 with layer
	// attribution and retry metadata.
	for i := 0; i < 5; i++ {
		code, body := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
			"tenant": "acme", "user": "bob", "api": "login",
		})
		if code != 200 || body["allowed"] != true {
			t.Fatalf("call %d: code=%d body=%v", i, code, body)
		}
	}
	code, body := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
		"tenant": "acme", "user": "bob", "api": "login",
	})
	if code != 429 {
		t.Fatalf("code=%d want 429 body=%v", code, body)
	}
	if body["allowed"] != false {
		t.Fatal("denied response allowed field")
	}
	if body["limited_by_layer"] != "api" {
		t.Fatalf("limiting layer=%v want api (login bucket is tightest)",
			body["limited_by_layer"])
	}
	retry, _ := body["retry_after_ms"].(float64)
	if retry <= 0 {
		t.Fatalf("retry_after_ms=%v must be >0", body["retry_after_ms"])
	}
	buckets, _ := body["buckets"].([]any)
	if len(buckets) != 3 {
		t.Fatalf("buckets=%v want 3 rows", buckets)
	}
	for _, row := range buckets {
		m := row.(map[string]any)
		if m["remaining_mt"] == nil || m["refill_mtps"] == nil {
			t.Fatalf("bucket row missing auditable fields: %v", m)
		}
	}
}

func TestClockSkewReported(t *testing.T) {
	h := newHarness(t)
	// Offset the LOCAL clock by +2500ms while Redis clock stays fixed. The
	// decision itself must be unaffected; skew is observability only.
	h.clock.t = h.clock.t.Add(2500 * time.Millisecond)
	code, body := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
		"tenant": "acme", "user": "bob", "api": "docs",
	})
	if code != 200 {
		t.Fatalf("code=%d body=%v", code, body)
	}
	skew, _ := body["clock_skew_ms"].(float64)
	if skew != 2500 {
		t.Fatalf("clock_skew_ms=%v want 2500", body["clock_skew_ms"])
	}
	if body["clock_source"] != "redis:TIME" {
		t.Fatalf("clock_source=%v", body["clock_source"])
	}
}

func TestIdempotentRetryDoesNotDoubleCharge(t *testing.T) {
	h := newHarness(t)
	// docs has a large burst, but use login (5 burst) with an idem key: many
	// retries must all return the ORIGINAL allow and leave 4 tokens.
	for i := 0; i < 10; i++ {
		code, body := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
			"tenant": "acme", "user": "carol", "api": "login",
			"idempotency_key": "order-77",
		})
		if code != 200 {
			t.Fatalf("retry %d code=%d body=%v", i, code, body)
		}
		if body["allowed"] != true {
			t.Fatalf("replay denied: %v", body)
		}
	}
	// Distinct request (new idem key) for same user: 4 more allowed then deny.
	for i := 0; i < 4; i++ {
		code, _ := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
			"tenant": "acme", "user": "carol", "api": "login",
			"idempotency_key": "fresh-" + strconv.Itoa(i),
		})
		if code != 200 {
			t.Fatalf("fresh call %d code=%d", i, code)
		}
	}
	code, _ := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
		"tenant": "acme", "user": "carol", "api": "login",
		"idempotency_key": "fresh-deny",
	})
	if code != 429 {
		t.Fatalf("6th distinct call code=%d want 429", code)
	}
}

func TestFailClosedHighRiskWhenRedisDown(t *testing.T) {
	h := newHarness(t)
	// login is high risk => fail closed. Close Redis client to force errors.
	_ = h.rdb.Close()
	code, body := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
		"tenant": "acme", "user": "bob", "api": "login",
	})
	if code != 503 {
		t.Fatalf("code=%d want 503 body=%v", code, body)
	}
	if body["allowed"] != false {
		t.Fatal("fail-closed must deny")
	}
}

func TestFailOpenLowRiskWhenRedisDown(t *testing.T) {
	h := newHarness(t)
	// search/docs are low risk => fail open with degraded flag.
	_ = h.rdb.Close()
	code, body := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
		"tenant": "acme", "user": "bob", "api": "search",
	})
	if code != 200 {
		t.Fatalf("code=%d want 200 body=%v", code, body)
	}
	if body["allowed"] != true || body["degraded"] != true {
		t.Fatalf("fail-open response: %v", body)
	}
}

func TestBusinessDedupEndpoint(t *testing.T) {
	h := newHarness(t)
	first := map[string]any{
		"tenant": "acme", "dedup_key": "tx-1", "payload": "transfer 10",
	}
	code, body := h.do(t, "POST", "/api/v1/business/dedup", first)
	if code != 200 || body["first"] != true {
		t.Fatalf("first: %d %v", code, body)
	}
	code, body = h.do(t, "POST", "/api/v1/business/dedup", first)
	if code != 200 || body["duplicate"] != true {
		t.Fatalf("dup: %d %v", code, body)
	}
	// Different payload, same key -> conflict.
	code, body = h.do(t, "POST", "/api/v1/business/dedup", map[string]any{
		"tenant": "acme", "dedup_key": "tx-1", "payload": "transfer 999",
	})
	if code != 409 {
		t.Fatalf("conflict code=%d body=%v", code, body)
	}
}

func TestAdminPolicyChangeTakesEffect(t *testing.T) {
	h := newHarness(t)
	// Tighten docs policy via admin API; refresh cache; burst must shrink.
	code, body := h.do(t, "GET", "/admin/v1/policies", nil)
	if code != 200 {
		t.Fatal(body)
	}
	var docsID int64
	for _, p := range body["policies"].([]any) {
		m := p.(map[string]any)
		if m["name"] == "api-docs" {
			docsID = int64(m["id"].(float64))
		}
	}
	if docsID == 0 {
		t.Fatal("api-docs policy not found")
	}
	code, _ = h.do(t, "PUT", "/admin/v1/policies/"+strconv.FormatInt(docsID, 10), map[string]any{
		"name": "api-docs", "capacity_mt": 3000, "refill_mtps": 1000,
	})
	if code != 200 {
		t.Fatalf("update code=%d", code)
	}
	if code, _ = h.do(t, "POST", "/admin/v1/cache/refresh", nil); code != 200 {
		t.Fatalf("refresh code=%d", code)
	}
	allowed := 0
	for i := 0; i < 5; i++ {
		c, b := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
			"tenant": "z", "user": "u", "api": "docs",
		})
		if c == 200 && b["allowed"] == true {
			allowed++
		}
	}
	// Other layers: tenant default 100 burst, user 20; docs now 3.
	if allowed != 3 {
		t.Fatalf("after policy change admitted=%d want 3", allowed)
	}
}

func TestCostTokensValidation(t *testing.T) {
	h := newHarness(t)
	code, _ := h.do(t, "POST", "/api/v1/ratelimit/check", map[string]any{
		"tenant": "a", "api": "docs", "cost_tokens": -1,
	})
	if code != 400 {
		t.Fatalf("negative cost code=%d want 400", code)
	}
}
