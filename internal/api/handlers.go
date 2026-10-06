package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"ratelimit-platform/internal/policy"
	"ratelimit-platform/internal/ratelimit"
)

func (s *Server) health(c *gin.Context) {
	if s.RedisHealth != nil {
		if err := s.RedisHealth(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "redis": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// clock exposes the authoritative clock so callers can audit clock skew.
// Decision math never trusts the caller clock; this endpoint exists for
// observability and for diagnosing cross-node drift.
func (s *Server) clock(c *gin.Context) {
	local := s.nowLocal()
	// Reuse the engine's Redis call budget by issuing a trivial script-less
	// round-trip through the health probe; the engine returns Redis time
	// from each decision anyway.
	if s.RedisHealth == nil {
		c.JSON(http.StatusOK, gin.H{"clock_source": "local", "local_now_ms": local.UnixMilli()})
		return
	}
	redisNow, err := s.redisTime(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"clock_source": "redis", "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"clock_source":  "redis:TIME",
		"redis_now_ms":  redisNow.UnixMilli(),
		"local_now_ms":  local.UnixMilli(),
		"clock_skew_ms": local.UnixMilli() - redisNow.UnixMilli(),
	})
}

// RedisTime is implemented in handlers_redis.go; this indirection keeps this
// file free of go-redis types.
//
//go:noinline
func (s *Server) redisTime(ctx context.Context) (time.Time, error) {
	if s.redisTimeFn == nil {
		return time.Time{}, errors.New("redis time probe not configured")
	}
	return s.redisTimeFn(ctx)
}

// check is the primary decision endpoint.
func (s *Server) check(c *gin.Context) {
	var req checkRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		badRequest(c, "invalid JSON body: "+err.Error())
		return
	}
	if req.Tenant == "" || req.API == "" {
		badRequest(c, "tenant and api are required")
		return
	}

	costMT, err := parseCost(req.CostTokens)
	if err != nil {
		badRequest(c, err.Error())
		return
	}

	snap := s.Resolver.Snapshot()
	res, err := policy.Resolve(snap, req.Tenant, req.User, req.API)
	if err != nil {
		switch {
		case errors.Is(err, policy.ErrNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		case errors.Is(err, policy.ErrEndpointDisabled):
			c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		}
		return
	}

	specs := make([]ratelimit.BucketSpec, 0, len(res.Buckets))
	for _, b := range res.Buckets {
		specs = append(specs, ratelimit.BucketSpec{
			Layer:      b.Layer,
			Name:       b.Subject,
			PolicyID:   b.Policy.ID,
			Capacity:   b.Policy.CapacityMT,
			RefillRate: b.Policy.RefillMTPS,
			Version:    b.Policy.Version,
			IdleTTL:    idleTTL(b.Policy.CapacityMT, b.Policy.RefillMTPS, s.IdleFloor),
		})
	}

	fp := fingerprint(req.Tenant, req.User, req.API, costMT)
	d, err := s.Engine.Decide(c.Request.Context(), ratelimit.CheckRequest{
		Tenant:         req.Tenant,
		User:           req.User,
		API:            req.API,
		Cost:           costMT,
		IdempotencyKey: req.IdempotencyKey,
		Fingerprint:    fp,
		Buckets:        specs,
	})

	localNow := s.nowLocal()

	if err != nil {
		if errors.Is(err, ratelimit.ErrConflict) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "idempotency key was already used with a different request payload",
			})
			return
		}
		if errors.Is(err, ratelimit.ErrCostTooLarge) {
			badRequest(c, err.Error())
			return
		}
		// Redis failure: open or close per the INTERFACE risk configuration.
		if errors.Is(err, ratelimit.ErrUnavailable) {
			s.failByRisk(c, res.Endpoint.FailPolicy, localNow, costMT)
			return
		}
		badRequest(c, err.Error())
		return
	}

	resp := checkResponse{
		Allowed:     d.Allowed,
		Replayed:    d.Replayed,
		ClockSource: "redis:TIME",
		RedisNowMs:  d.Now.UnixMilli(),
		LocalNowMs:  localNow.UnixMilli(),
		ClockSkewMs: localNow.UnixMilli() - d.Now.UnixMilli(),
		CostMT:      costMT,
		Buckets:     toDTOs(d.Buckets),
	}
	if !d.Allowed {
		resp.LimitedLayer = d.LimitedLayer
		resp.LimitedName = d.LimitedName
		resp.NeverRetry = d.NeverRetry
		if !d.NeverRetry {
			resp.RetryAfterMs = d.Wait.Milliseconds()
		}
		c.Header("Retry-After", retryAfterHeader(d))
		c.JSON(http.StatusTooManyRequests, resp)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// failByRisk applies the interface-level Redis failure posture.
func (s *Server) failByRisk(c *gin.Context, p policy.FailPolicy, local time.Time, costMT int64) {
	switch p {
	case policy.FailOpen:
		// Low-risk interface: availability wins; the request is allowed but
		// the response is explicitly marked degraded so callers/auditors see
		// that no accounting happened.
		c.JSON(http.StatusOK, checkResponse{
			Allowed:     true,
			ClockSource: "unavailable",
			LocalNowMs:  local.UnixMilli(),
			CostMT:      costMT,
			Degraded:    true,
		})
	default:
		// High-risk interface: fail closed.
		c.Header("Retry-After", "1")
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"allowed":          false,
			"clock_source":     "unavailable",
			"local_now_ms":     local.UnixMilli(),
			"cost_mt":          costMT,
			"limited_by_layer": "ratelimiter",
			"retry_after_ms":   1000,
			"error":            "rate limiter unavailable; interface policy is fail-closed",
		})
	}
}

func retryAfterHeader(d ratelimit.Decision) string {
	if d.NeverRetry {
		return ""
	}
	secs := (d.Wait + time.Second - 1) / time.Second
	if secs < 1 {
		secs = 1
	}
	return fmt.Sprintf("%ds", int64(secs))
}

func parseCost(c *float64) (int64, error) {
	if c == nil {
		return 1000, nil
	}
	if *c <= 0 {
		return 0, errors.New("cost_tokens must be > 0")
	}
	if *c > 1_000_000 {
		return 0, errors.New("cost_tokens exceeds 1,000,000")
	}
	// Accept at most 3 decimal places; convert exactly, no float drift beyond
	// the 0.001 resolution.
	mt := math.Round(*c * 1000)
	if mt < 1 {
		return 0, errors.New("cost_tokens resolution is 0.001 tokens")
	}
	return int64(mt), nil
}

func badRequest(c *gin.Context, msg string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": msg})
}
