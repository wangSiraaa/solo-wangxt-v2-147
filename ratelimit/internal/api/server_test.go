package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"example.com/ratelimit/internal/limiter"
	"example.com/ratelimit/internal/policy"
)

func newTestServer(t *testing.T, seed []policy.Policy) (*Server, *miniredis.Miniredis, *redis.Client, *policy.MemStore) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := policy.NewMemStore()
	for _, p := range seed {
		if _, err := store.Create(context.Background(), p); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	srv := NewServer(Options{
		Limiter:        limiter.New(rdb),
		Store:          store,
		MaxIdemTTL:     5 * time.Second,
		DefaultIdemTTL: 2 * time.Second,
		RedisTime:      func(ctx context.Context) (time.Time, error) { return rdb.Time(ctx).Result() },
	})
	t.Cleanup(func() { rdb.Close() })
	return srv, mr, rdb, store
}

func defaultSeed() []policy.Policy {
	return []policy.Policy{
		{Scope: policy.ScopeTenant, ScopeKey: "t1", Capacity: 1000, RefillPerSec: 0, Enabled: true},
		{Scope: policy.ScopeTenant, ScopeKey: "*", Capacity: 1000, RefillPerSec: 1000, Enabled: true},
		{Scope: policy.ScopeUser, ScopeKey: "t1:u1", Capacity: 1, RefillPerSec: 0, Enabled: true},
		{Scope: policy.ScopeUser, ScopeKey: "*", Capacity: 100, RefillPerSec: 0, Enabled: true},
		{Scope: policy.ScopeEndpoint, ScopeKey: "/pay", Capacity: 1000, RefillPerSec: 0, FailOpen: false, Enabled: true},
		{Scope: policy.ScopeEndpoint, ScopeKey: "/safe", Capacity: 1000, RefillPerSec: 0, FailOpen: true, Enabled: true},
		{Scope: policy.ScopeEndpoint, ScopeKey: "*", Capacity: 1000, RefillPerSec: 0, Enabled: true},
	}
}

func doRequest(t *testing.T, srv *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	return w
}

func checkBody(tenant, user, endpoint string) string {
	return fmt.Sprintf(`{"tenant_id":%q,"user_id":%q,"endpoint":%q}`, tenant, user, endpoint)
}

// 拒绝响应必须指明限制来自哪一层。
func TestCheckLayerAttribution(t *testing.T) {
	srv, _, _, _ := newTestServer(t, defaultSeed())

	w := doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t1", "u1", "/pay"))
	if w.Code != http.StatusOK {
		t.Fatalf("first check: %d %s", w.Code, w.Body)
	}

	w = doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t1", "u1", "/pay"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second check: %d %s", w.Code, w.Body)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["limited_by"] != "user" {
		t.Fatalf("limited_by = %v, want user", resp["limited_by"])
	}
	// rate=0：当前策略下无法满足
	if resp["retry_after_ms"].(float64) != -1 {
		t.Fatalf("retry_after_ms = %v, want -1", resp["retry_after_ms"])
	}

	// 其它用户不受 u1 的用户桶影响
	w = doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t1", "u2", "/pay"))
	if w.Code != http.StatusOK {
		t.Fatalf("other user should pass: %d %s", w.Code, w.Body)
	}
}

// 策略变更：PUT 后立即生效（不等待缓存 TTL）。
// 注意语义：令牌只随时间补充，调大容量不会追溯发放令牌；
// 因此变更时同时设置补充速率，并快进时钟后再验证放行。
func TestPolicyChangeTakesEffectImmediately(t *testing.T) {
	srv, _, _, _ := newTestServer(t, defaultSeed())

	w := doRequest(t, srv, http.MethodPost, "/v1/policies",
		`{"scope":"tenant","scope_key":"t2","capacity":2,"refill_per_sec":0,"enabled":true}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	var created policy.Policy
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if w := doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t2", "u9", "/pay")); w.Code != http.StatusOK {
			t.Fatalf("check %d: %d %s", i, w.Code, w.Body)
		}
	}
	w = doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t2", "u9", "/pay"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("should be denied at capacity 2: %d %s", w.Code, w.Body)
	}
	var denied map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &denied); err != nil {
		t.Fatal(err)
	}
	if denied["limited_by"] != "tenant" {
		t.Fatalf("limited_by = %v, want tenant", denied["limited_by"])
	}

	// 调大容量并设置补充速率 → 立即生效（无需等缓存 TTL），补充 100ms 后放行
	w = doRequest(t, srv, http.MethodPut, fmt.Sprintf("/v1/policies/%d", created.ID),
		`{"capacity":5,"refill_per_sec":100,"fail_open":false,"enabled":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update: %d %s", w.Code, w.Body)
	}
	time.Sleep(100 * time.Millisecond) // 100/s * 0.1s = +10 令牌，封顶 5
	if w := doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t2", "u9", "/pay")); w.Code != http.StatusOK {
		t.Fatalf("after policy change should pass: %d %s", w.Code, w.Body)
	}

	// 停用精确策略后回退到通配策略（容量 1000、速率 1000/s），不再被小容量限制。
	// 注意：桶状态按租户键保留，回退后沿用同一桶，只是参数换成通配策略的。
	w = doRequest(t, srv, http.MethodPut, fmt.Sprintf("/v1/policies/%d", created.ID),
		`{"capacity":5,"refill_per_sec":100,"fail_open":false,"enabled":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("disable: %d %s", w.Code, w.Body)
	}
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 10; i++ {
		if w := doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t2", "u9", "/pay")); w.Code != http.StatusOK {
			t.Fatalf("disabled policy should not limit: %d %s", w.Code, w.Body)
		}
	}
}

// Redis 故障 + fail_open=false（高风险接口）→ 拒绝。
func TestRedisDownFailClosed(t *testing.T) {
	srv, mr, _, _ := newTestServer(t, defaultSeed())
	mr.Close()

	w := doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t1", "u2", "/pay"))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("fail-closed: %d %s", w.Code, w.Body)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["allowed"] != false || resp["limited_by"] != "availability" {
		t.Fatalf("unexpected body: %v", resp)
	}
}

// Redis 故障 + fail_open=true（低风险接口）→ 放行并标记降级。
func TestRedisDownFailOpen(t *testing.T) {
	srv, mr, _, _ := newTestServer(t, defaultSeed())
	mr.Close()

	w := doRequest(t, srv, http.MethodPost, "/v1/check", checkBody("t1", "u2", "/safe"))
	if w.Code != http.StatusOK {
		t.Fatalf("fail-open: %d %s", w.Code, w.Body)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["allowed"] != true || resp["degraded"] != true {
		t.Fatalf("unexpected body: %v", resp)
	}
}

// 请求校验。
func TestCheckValidation(t *testing.T) {
	srv, _, _, _ := newTestServer(t, defaultSeed())

	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing tenant", `{"user_id":"u1","endpoint":"/pay"}`, http.StatusBadRequest},
		{"missing user", `{"tenant_id":"t1","endpoint":"/pay"}`, http.StatusBadRequest},
		{"missing endpoint", `{"tenant_id":"t1","user_id":"u1"}`, http.StatusBadRequest},
		{"negative cost", `{"tenant_id":"t1","user_id":"u1","endpoint":"/pay","cost":-1}`, http.StatusBadRequest},
		{"huge cost", `{"tenant_id":"t1","user_id":"u1","endpoint":"/pay","cost":1001}`, http.StatusBadRequest},
		{"ok", `{"tenant_id":"t1","user_id":"u1","endpoint":"/pay","cost":1}`, http.StatusOK},
		{"cost omitted defaults to 1", `{"tenant_id":"t1","user_id":"u2","endpoint":"/pay"}`, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if w := doRequest(t, srv, http.MethodPost, "/v1/check", tc.body); w.Code != tc.want {
				t.Fatalf("%d %s, want %d", w.Code, w.Body, tc.want)
			}
		})
	}
}

// 幂等窗口钳制到服务端上限：请求 999s 只能得到 ≤5s（测试配置的上限）的缓存。
func TestIdempotencyTTLClamped(t *testing.T) {
	srv, _, rdb, _ := newTestServer(t, defaultSeed())

	body := `{"tenant_id":"t1","user_id":"u5","endpoint":"/pay","idempotency_key":"abc","idempotency_ttl_ms":999000}`
	if w := doRequest(t, srv, http.MethodPost, "/v1/check", body); w.Code != http.StatusOK {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	ttl, err := rdb.PTTL(context.Background(), "rl:{t1}:idem:abc").Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("idem ttl = %v, want in (0, 5s] (clamped to server max)", ttl)
	}
}

// 时钟源可核对：/v1/clock 返回 Redis 时间与本机时间的偏差。
func TestClockEndpoint(t *testing.T) {
	srv, _, _, _ := newTestServer(t, defaultSeed())
	w := doRequest(t, srv, http.MethodGet, "/v1/clock", "")
	if w.Code != http.StatusOK {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["redis_now_ms"].(float64) <= 0 {
		t.Fatalf("bad response: %v", resp)
	}
}

// 策略 CRUD 基础路径。
func TestPolicyCRUD(t *testing.T) {
	srv, _, _, _ := newTestServer(t, nil)

	w := doRequest(t, srv, http.MethodPost, "/v1/policies",
		`{"scope":"endpoint","scope_key":"/orders","capacity":50,"refill_per_sec":5,"fail_open":true,"enabled":true}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	var p policy.Policy
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.ID == 0 || !p.FailOpen {
		t.Fatalf("bad created policy: %+v", p)
	}

	// 重复 scope+scope_key → 409
	w = doRequest(t, srv, http.MethodPost, "/v1/policies",
		`{"scope":"endpoint","scope_key":"/orders","capacity":50,"refill_per_sec":5}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d, want 409", w.Code)
	}

	w = doRequest(t, srv, http.MethodGet, "/v1/policies?scope=endpoint", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "/orders") {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}

	w = doRequest(t, srv, http.MethodDelete, fmt.Sprintf("/v1/policies/%d", p.ID), "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", w.Code)
	}
	w = doRequest(t, srv, http.MethodDelete, fmt.Sprintf("/v1/policies/%d", p.ID), "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete again: %d, want 404", w.Code)
	}

	// 非法策略 → 400
	w = doRequest(t, srv, http.MethodPost, "/v1/policies",
		`{"scope":"endpoint","scope_key":"/x","capacity":0,"refill_per_sec":5}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid: %d, want 400", w.Code)
	}
}
