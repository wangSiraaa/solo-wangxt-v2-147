// Package api wires HTTP handlers to the policy resolver and rate-limit
// engine. No frontend assets are served.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"ratelimit-platform/internal/policy"
	"ratelimit-platform/internal/ratelimit"
)

// Server holds handler dependencies.
type Server struct {
	Resolver  policy.Resolver
	Engine    *ratelimit.Engine
	Clock     func() time.Time // local clock, used only for skew reporting
	IdleFloor time.Duration
	// RedisHealth is probed by /clock and the fail policy path.
	RedisHealth func() error
	// redisTimeFn returns the Redis server time (authoritative clock).
	redisTimeFn func(ctx context.Context) (time.Time, error)
}

// SetRedisTimeFn wires the Redis TIME probe.
func (s *Server) SetRedisTimeFn(fn func(ctx context.Context) (time.Time, error)) {
	s.redisTimeFn = fn
}

// checkRequest is the JSON body of POST /api/v1/ratelimit/check.
type checkRequest struct {
	Tenant         string   `json:"tenant"`
	User           string   `json:"user"`
	API            string   `json:"api"`
	CostTokens     *float64 `json:"cost_tokens"`
	IdempotencyKey string   `json:"idempotency_key"`
}

// bucketDTO is the per-layer state returned to the caller.
type bucketDTO struct {
	Layer        string `json:"layer"`
	Name         string `json:"name"`
	PolicyID     int64  `json:"policy_id"`
	Remaining    int64  `json:"remaining_mt"`
	RemainingTok string `json:"remaining_tokens"`
	CapacityMT   int64  `json:"capacity_mt"`
	RefillMTPS   int64  `json:"refill_mtps"`
}

// checkResponse is the decision envelope.
type checkResponse struct {
	Allowed      bool        `json:"allowed"`
	Replayed     bool        `json:"replayed,omitempty"`
	ClockSource  string      `json:"clock_source"`
	RedisNowMs   int64       `json:"redis_now_ms"`
	LocalNowMs   int64       `json:"local_now_ms"`
	ClockSkewMs  int64       `json:"clock_skew_ms"`
	CostMT       int64       `json:"cost_mt"`
	LimitedLayer string      `json:"limited_by_layer,omitempty"`
	LimitedName  string      `json:"limited_by_name,omitempty"`
	RetryAfterMs int64       `json:"retry_after_ms,omitempty"`
	NeverRetry   bool        `json:"never_retry,omitempty"`
	Buckets      []bucketDTO `json:"buckets"`
	// Degraded is set when Redis was unavailable and the interface risk
	// configuration chose fail-open.
	Degraded bool `json:"degraded,omitempty"`
}

// Register mounts decision/read routes on r. Admin routes are mounted via
// AdminServer.RegisterAdmin.
func (s *Server) Register(r *gin.Engine) {
	r.GET("/healthz", s.health)
	r.GET("/api/v1/clock", s.clock)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/ratelimit/check", s.check)
		v1.POST("/business/dedup", s.businessDedup)
	}
}

func (s *Server) nowLocal() time.Time {
	if s.Clock != nil {
		return s.Clock().UTC()
	}
	return time.Now().UTC()
}

// fingerprint builds the canonical payload hash binding an idempotency key to
// exactly one decision request.
func fingerprint(tenant, user, api string, costMT int64) string {
	canon := strings.Join([]string{"v1", tenant, user, api, itoa(costMT)}, "\n")
	sum := sha256.Sum256([]byte(canon))
	return hex.EncodeToString(sum[:])
}

func itoa(n int64) string { return fmt.Sprintf("%d", n) }

// idleTTL bounds bucket key lifetime: shortly after a fully drained bucket
// would refill to capacity, plus a floor. Capped at 24h.
func idleTTL(capacityMT, rateMTPS int64, floor time.Duration) time.Duration {
	if rateMTPS <= 0 {
		return 24 * time.Hour
	}
	ms := capacityMT*1000/rateMTPS + floor.Milliseconds()
	d := time.Duration(ms) * time.Millisecond
	if d > 24*time.Hour {
		d = 24 * time.Hour
	}
	return d
}

func toDTOs(views []ratelimit.BucketView) []bucketDTO {
	out := make([]bucketDTO, 0, len(views))
	for _, v := range views {
		out = append(out, bucketDTO{
			Layer:        v.Layer,
			Name:         v.Name,
			PolicyID:     v.PolicyID,
			Remaining:    v.Remaining,
			RemainingTok: mtToToken(v.Remaining),
			CapacityMT:   v.Capacity,
			RefillMTPS:   v.RefillRate,
		})
	}
	return out
}

func mtToToken(mt int64) string {
	return fmt.Sprintf("%d.%03d", mt/1000, mt%1000)
}
