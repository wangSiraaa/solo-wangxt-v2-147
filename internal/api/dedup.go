package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"ratelimit-platform/internal/ratelimit"
)

type dedupRequest struct {
	Tenant     string `json:"tenant"`
	DedupKey   string `json:"dedup_key"`
	Payload    string `json:"payload"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

// businessDedup claims a BUSINESS dedup window. It is deliberately separate
// from /ratelimit/check:
//   - namespace rl:dedup: vs decision keys rl:idem:
//   - long TTL (hours, bounded by request/cap) vs short decision replay TTL
//   - semantics: "already accepted business request" vs "replayed decision"
func (s *Server) businessDedup(c *gin.Context) {
	var req dedupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		badRequest(c, "invalid JSON body: "+err.Error())
		return
	}
	if req.Tenant == "" || req.DedupKey == "" || req.Payload == "" {
		badRequest(c, "tenant, dedup_key and payload are required")
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if req.TTLSeconds == 0 {
		ttl = 0 // engine default
	} else if req.TTLSeconds < 1 || req.TTLSeconds > 7*24*3600 {
		badRequest(c, "ttl_seconds must be between 1 and 604800")
		return
	}

	sum := sha256.Sum256([]byte(strings.TrimSpace(req.Payload)))
	fp := hex.EncodeToString(sum[:])

	res, err := s.Engine.BusinessDedup(c.Request.Context(), req.Tenant, req.DedupKey, fp, ttl)
	if err != nil {
		if errors.Is(err, ratelimit.ErrConflict) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "dedup key reused with a different payload",
			})
			return
		}
		if errors.Is(err, ratelimit.ErrUnavailable) {
			// Business dedup is a safety mechanism: never silently allow
			// duplicates when Redis is down.
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "dedup backend unavailable",
			})
			return
		}
		badRequest(c, err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"first":           res.First,
		"duplicate":       res.Duplicate,
		"clock_source":    "redis:TIME",
		"redis_now_ms":    res.Now.UnixMilli(),
		"dedup_namespace": "rl:dedup",
	})
}
