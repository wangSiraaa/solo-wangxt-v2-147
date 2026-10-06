// Package api 提供限流判定与策略管理的 HTTP API。
package api

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"example.com/ratelimit/internal/limiter"
	"example.com/ratelimit/internal/policy"
)

// maxCost 是单次判定允许消耗的最大令牌数，防止异常大 cost 打空桶。
const maxCost = 1000.0

// maxIdemKeyLen 限制幂等键长度，避免键无限膨胀。
const maxIdemKeyLen = 200

type Server struct {
	limiter    *limiter.Limiter
	store      policy.Store
	resolver   *policy.Resolver
	maxIdemTTL time.Duration
	defIdemTTL time.Duration
	redisTime  func(ctx context.Context) (time.Time, error)

	engine *gin.Engine
}

type Options struct {
	Limiter        *limiter.Limiter
	Store          policy.Store
	CacheTTL       time.Duration // 策略缓存 TTL，默认 5s
	MaxIdemTTL     time.Duration // 判定幂等窗口上限，默认 60s（有界，不无限缓存）
	DefaultIdemTTL time.Duration // 默认幂等窗口，默认 3s
	RedisTime      func(ctx context.Context) (time.Time, error)
}

func NewServer(opts Options) *Server {
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = 5 * time.Second
	}
	if opts.MaxIdemTTL <= 0 {
		opts.MaxIdemTTL = time.Minute
	}
	if opts.DefaultIdemTTL <= 0 {
		opts.DefaultIdemTTL = 3 * time.Second
	}
	s := &Server{
		limiter:    opts.Limiter,
		store:      opts.Store,
		resolver:   policy.NewResolver(opts.Store, opts.CacheTTL),
		maxIdemTTL: opts.MaxIdemTTL,
		defIdemTTL: opts.DefaultIdemTTL,
		redisTime:  opts.RedisTime,
	}
	e := gin.New()
	e.Use(gin.Recovery())
	v1 := e.Group("/v1")
	v1.POST("/check", s.check)
	v1.POST("/policies", s.createPolicy)
	v1.GET("/policies", s.listPolicies)
	v1.PUT("/policies/:id", s.updatePolicy)
	v1.DELETE("/policies/:id", s.deletePolicy)
	v1.GET("/healthz", s.healthz)
	v1.GET("/clock", s.clock)
	s.engine = e
	return s
}

func (s *Server) Handler() http.Handler { return s.engine }

// ---- 判定 ----

type checkRequest struct {
	TenantID         string  `json:"tenant_id" binding:"required"`
	UserID           string  `json:"user_id" binding:"required"`
	Endpoint         string  `json:"endpoint" binding:"required"`
	Cost             float64 `json:"cost"`               // 缺省 1
	IdempotencyKey   string  `json:"idempotency_key"`    // 可选：判定重试去重
	IdempotencyTTLMs int64   `json:"idempotency_ttl_ms"` // 可选：幂等窗口，钳制到上限
}

func (s *Server) check(c *gin.Context) {
	var req checkRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cost := req.Cost
	if cost == 0 {
		cost = 1
	}
	if cost < 0 || cost > maxCost {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cost must be in (0, 1000]"})
		return
	}
	if len(req.IdempotencyKey) > maxIdemKeyLen {
		c.JSON(http.StatusBadRequest, gin.H{"error": "idempotency_key too long"})
		return
	}
	ctx := c.Request.Context()

	// 解析三层策略；未配置策略的层级不参与判定
	var buckets []limiter.Bucket
	failOpen := false

	tp, ok, err := s.resolver.Resolve(ctx, policy.ScopeTenant, policy.TenantKeys(req.TenantID)...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "policy store unavailable"})
		return
	}
	if ok {
		buckets = append(buckets, limiter.Bucket{
			Layer: limiter.LayerTenant, Key: "rl:{" + req.TenantID + "}:t",
			Capacity: tp.Capacity, RatePerSec: tp.RefillPerSec,
		})
	}

	up, ok, err := s.resolver.Resolve(ctx, policy.ScopeUser, policy.UserKeys(req.TenantID, req.UserID)...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "policy store unavailable"})
		return
	}
	if ok {
		buckets = append(buckets, limiter.Bucket{
			Layer: limiter.LayerUser, Key: "rl:{" + req.TenantID + "}:u:" + req.UserID,
			Capacity: up.Capacity, RatePerSec: up.RefillPerSec,
		})
	}

	ep, ok, err := s.resolver.Resolve(ctx, policy.ScopeEndpoint, policy.EndpointKeys(req.Endpoint)...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "policy store unavailable"})
		return
	}
	if ok {
		failOpen = ep.FailOpen
		buckets = append(buckets, limiter.Bucket{
			Layer: limiter.LayerEndpoint, Key: "rl:{" + req.TenantID + "}:e:" + req.Endpoint,
			Capacity: ep.Capacity, RatePerSec: ep.RefillPerSec,
		})
	}

	// 判定幂等：窗口有界（钳制到 maxIdemTTL），仅缓存带显式键的判定
	idemTTL := s.defIdemTTL
	if req.IdempotencyTTLMs > 0 {
		idemTTL = time.Duration(req.IdempotencyTTLMs) * time.Millisecond
		if idemTTL > s.maxIdemTTL {
			idemTTL = s.maxIdemTTL
		}
	}
	idemKey := ""
	if req.IdempotencyKey != "" {
		idemKey = "rl:{" + req.TenantID + "}:idem:" + req.IdempotencyKey
	}

	d, err := s.limiter.Check(ctx, buckets, cost, idemKey, idemTTL)
	if err != nil {
		// Redis 故障：按接口风险配置开放或拒绝
		if failOpen {
			c.JSON(http.StatusOK, gin.H{"allowed": true, "degraded": true})
		} else {
			c.Header("Retry-After", "1")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"allowed":        false,
				"limited_by":     "availability",
				"retry_after_ms": 1000,
				"error":          "rate limiter unavailable",
			})
		}
		return
	}

	remaining := gin.H{}
	for layer, v := range d.Remaining {
		remaining[string(layer)] = v
	}
	if d.Allowed {
		c.JSON(http.StatusOK, gin.H{
			"allowed":   true,
			"replay":    d.Replay,
			"remaining": remaining,
		})
		return
	}

	body := gin.H{
		"allowed":    false,
		"limited_by": string(d.LimitedBy),
		"remaining":  remaining,
	}
	if d.RetryAfter >= 0 {
		ms := d.RetryAfter.Milliseconds()
		body["retry_after_ms"] = ms
		sec := int64(math.Ceil(float64(ms) / 1000))
		if sec < 1 {
			sec = 1
		}
		c.Header("Retry-After", strconv.FormatInt(sec, 10))
	} else {
		body["retry_after_ms"] = -1 // 当前策略下永远无法满足（如 cost 超过容量）
	}
	c.JSON(http.StatusTooManyRequests, body)
}

// ---- 策略管理 ----

func (s *Server) createPolicy(c *gin.Context) {
	var p policy.Policy
	if err := c.ShouldBindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := p.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	created, err := s.store.Create(c.Request.Context(), p)
	if errors.Is(err, policy.ErrConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.resolver.Invalidate()
	c.JSON(http.StatusCreated, created)
}

func (s *Server) listPolicies(c *gin.Context) {
	list, err := s.store.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	scope := c.Query("scope")
	out := make([]policy.Policy, 0, len(list))
	for _, p := range list {
		if scope == "" || string(p.Scope) == scope {
			out = append(out, p)
		}
	}
	c.JSON(http.StatusOK, gin.H{"policies": out})
}

// updatePolicy 全量替换可变字段（capacity / refill_per_sec / fail_open / enabled），
// scope 与 scope_key 不可变。变更立即使缓存失效，下一次判定生效。
func (s *Server) updatePolicy(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var req struct {
		Capacity     float64 `json:"capacity"`
		RefillPerSec float64 `json:"refill_per_sec"`
		FailOpen     bool    `json:"fail_open"`
		Enabled      bool    `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx := c.Request.Context()
	p, err := s.store.Get(ctx, id)
	if errors.Is(err, policy.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "policy not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	p.Capacity = req.Capacity
	p.RefillPerSec = req.RefillPerSec
	p.FailOpen = req.FailOpen
	p.Enabled = req.Enabled
	if err := p.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	updated, err := s.store.Update(ctx, p)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.resolver.Invalidate()
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deletePolicy(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	err = s.store.Delete(c.Request.Context(), id)
	if errors.Is(err, policy.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "policy not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.resolver.Invalidate()
	c.Status(http.StatusNoContent)
}

// ---- 可观测 ----

func (s *Server) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// clock 暴露判定使用的时钟源（Redis TIME）与应用本机时间的偏差，便于核对。
func (s *Server) clock(c *gin.Context) {
	if s.redisTime == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "redis time source not configured"})
		return
	}
	appNow := time.Now()
	rt, err := s.redisTime(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "redis unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"clock_source": "redis TIME (authoritative clock used inside the Lua decision script)",
		"redis_now_ms": rt.UnixMilli(),
		"app_now_ms":   appNow.UnixMilli(),
		"skew_ms":      rt.Sub(appNow).Milliseconds(),
	})
}
