// Package ratelimit implements the multi-tenant token-bucket decision engine.
//
// Semantics (auditable):
//   - Standard token bucket. Capacity = burst; refill rate is independent;
//     buckets start FULL on first use.
//   - Integer arithmetic in millitokens (1 token = 1000 mT) and milliseconds;
//     refill = floor(elapsed_ms * rate_mtps / 1000). No floats touch state.
//   - Clock source for all state math is the Redis server TIME sampled inside
//     the atomic script. Caller clocks are reported for skew observability
//     only and can never change an accounting result.
//   - All relevant buckets are evaluated in ONE Lua script; a DENY mutates no
//     bucket and an ALLOW consumes cost from every bucket (all-or-nothing).
package ratelimit

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed decide.lua
var decideScriptSrc string

//go:embed dedup.lua
var dedupScriptSrc string

// Layer names returned to clients and used in keys/evaluation order.
const (
	LayerTenant = "tenant"
	LayerUser   = "user"
	LayerAPI    = "api"
)

// ErrUnavailable is returned when Redis cannot answer a decision.
// Callers apply the interface risk configuration (fail open / fail closed).
var ErrUnavailable = errors.New("ratelimit: redis unavailable")

// ErrConflict means an idempotency/dedup key was reused with a different
// request fingerprint.
var ErrConflict = errors.New("ratelimit: idempotency key fingerprint conflict")

// ErrCostTooLarge means the requested cost can never fit the burst capacity.
var ErrCostTooLarge = errors.New("ratelimit: cost exceeds bucket capacity")

// Limits on encoded values; they keep Lua integer math safely inside
// 2^53 even under extreme elapsed*rate products.
const (
	MaxCapacityMilliTokens = 1_000_000_000 // 1,000,000 tokens
	MaxRateMilliTPS        = 1_000_000_000 // 1,000,000 tokens/sec
)

// BucketSpec describes one layer bucket participating in a decision.
type BucketSpec struct {
	Layer      string // LayerTenant | LayerUser | LayerAPI
	Name       string // tenant id / user id / api id
	PolicyID   int64
	Capacity   int64 // millitokens (burst)
	RefillRate int64 // millitokens per second
	Version    int64 // policy version; change forces bucket re-clamp
	// IdleTTL is how long an untouched bucket is retained after it would be
	// full again; bounded so Redis does not grow without limit.
	IdleTTL time.Duration
}

// BucketView is post-decision state of one layer bucket.
type BucketView struct {
	Layer      string `json:"layer"`
	Name       string `json:"name"`
	PolicyID   int64  `json:"policy_id"`
	Remaining  int64  `json:"remaining_mt"`
	Capacity   int64  `json:"capacity_mt"`
	RefillRate int64  `json:"refill_mtps"`
}

// Decision is the outcome of one atomic check.
type Decision struct {
	Allowed bool
	// Now is the authoritative Redis clock sampled by the script.
	Now time.Time
	// Replayed is true when the answer came from the bounded decision
	// idempotency window rather than a fresh accounting pass.
	Replayed bool
	// Wait is the minimum wall time until the request may be retried.
	// 0 with Denied=true and NeverRetry=true means the rate is zero.
	Wait       time.Duration
	NeverRetry bool
	// LimitedLayer / LimitedName identify which layer denied (lowest
	// evaluated layer wins when several are empty at once).
	LimitedLayer string
	LimitedName  string
	Buckets      []BucketView
}

// RetryAfter is the value suitable for the Retry-After header (seconds,
// rounded up). Returns (0, false) when retry can never succeed.
func (d Decision) RetryAfter() (int, bool) {
	if d.Allowed || d.NeverRetry {
		return 0, d.NeverRetry == false && !d.Allowed
	}
	secs := (d.Wait + time.Second - 1) / time.Second
	if secs < 1 {
		secs = 1
	}
	return int(secs), true
}

// Engine executes decisions against Redis.
type Engine struct {
	rdb            redis.Scripter
	decideScript   *redis.Script
	dedupScript    *redis.Script
	idemAllowTTL   time.Duration
	idemDenyTTL    time.Duration
	dedupTTL       time.Duration
	decisionBudget time.Duration
}

// Config configures Engine.
type Config struct {
	// IdemAllowTTL / IdemDenyTTL bound the DECISION replay window. Denies are
	// cached briefly (a few seconds) to absorb immediate retries; they are
	// never cached "forever".
	IdemAllowTTL   time.Duration
	IdemDenyTTL    time.Duration
	DedupTTL       time.Duration
	DecisionBudget time.Duration // Redis call budget before ErrUnavailable
}

// NewEngine loads scripts against rdb. rdb is usually *redis.Client; in tests
// it can be a miniredis-backed client.
func NewEngine(rdb redis.Scripter, cfg Config) *Engine {
	if cfg.IdemAllowTTL == 0 {
		cfg.IdemAllowTTL = 60 * time.Second
	}
	if cfg.IdemDenyTTL == 0 {
		cfg.IdemDenyTTL = 5 * time.Second
	}
	if cfg.DedupTTL == 0 {
		cfg.DedupTTL = 24 * time.Hour
	}
	if cfg.DecisionBudget == 0 {
		cfg.DecisionBudget = 200 * time.Millisecond
	}
	return &Engine{
		rdb:            rdb,
		decideScript:   redis.NewScript(decideScriptSrc),
		dedupScript:    redis.NewScript(dedupScriptSrc),
		idemAllowTTL:   cfg.IdemAllowTTL,
		idemDenyTTL:    cfg.IdemDenyTTL,
		dedupTTL:       cfg.DedupTTL,
		decisionBudget: cfg.DecisionBudget,
	}
}

// Scripts returns loaded scripts (used by tests/miniredis warm-up).
func (e *Engine) Scripts() (*redis.Script, *redis.Script) { return e.decideScript, e.dedupScript }

func bucketKey(tenant string, b BucketSpec) string {
	// All keys of one decision share the hash tag {t:<tenant>} so a Redis
	// Cluster routes the multi-key EVAL to one slot. The suffix carries the
	// layer identity and fully-qualified subject.
	return "rl:bucket:{t:" + tenant + "}:" + b.Layer + ":" + b.Name
}

// idemKey is the DECISION replay key, distinct namespace from business dedup.
func idemKey(tenant, key string) string {
	return "rl:idem:{" + tenant + "}:" + key
}

// DedupKey is the BUSINESS dedup key: separate namespace and lifetime.
func DedupKey(tenant, key string) string {
	return "rl:dedup:{" + tenant + "}:" + key
}

var safeTokenChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.:@"

func validateToken(label, v string) error {
	if v == "" {
		return fmt.Errorf("ratelimit: empty %s", label)
	}
	if len(v) > 200 {
		return fmt.Errorf("ratelimit: %s too long", label)
	}
	if strings.ContainsAny(v, "{}") {
		return fmt.Errorf("ratelimit: %s must not contain braces", label)
	}
	for _, r := range v {
		if !strings.ContainsRune(safeTokenChars, r) {
			return fmt.Errorf("ratelimit: %s contains invalid character %q", label, r)
		}
	}
	return nil
}

func isLowerHex(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

// CheckRequest is one atomic rate-limit decision request.
type CheckRequest struct {
	Tenant string
	User   string // optional; empty means no user bucket
	API    string
	Cost   int64 // millitokens; 0 defaults to 1000 (one token)
	// IdempotencyKey scopes the bounded DECISION replay window. Empty disables.
	IdempotencyKey string
	// Fingerprint binds the key to the request payload (canonicalized by
	// the caller). Reuse with a different fingerprint is a 409.
	Fingerprint string
	Buckets     []BucketSpec
}

// parseFlatRows decodes the per-bucket row block shared by ALLOW/DENY.
func parseBucketRows(vals []interface{}, start int) (n int, buckets []BucketView, next int, err error) {
	if start >= len(vals) {
		return 0, nil, start, fmt.Errorf("ratelimit: short script reply")
	}
	n64, err := toInt64(vals[start])
	if err != nil {
		return 0, nil, start, err
	}
	n = int(n64)
	pos := start + 1
	if pos+n*5 > len(vals) {
		return 0, nil, pos, fmt.Errorf("ratelimit: truncated bucket rows")
	}
	for i := 0; i < n; i++ {
		rem, _ := toInt64(vals[pos])
		capM, _ := toInt64(vals[pos+1])
		rate, _ := toInt64(vals[pos+2])
		buckets = append(buckets, BucketView{
			Remaining:  rem,
			Capacity:   capM,
			RefillRate: rate,
			Layer:      fmt.Sprint(vals[pos+3]),
			Name:       fmt.Sprint(vals[pos+4]),
		})
		pos += 5
	}
	return n, buckets, pos, nil
}

func toInt64(v interface{}) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	case []byte:
		return strconv.ParseInt(string(x), 10, 64)
	}
	return 0, fmt.Errorf("ratelimit: unexpected reply type %T", v)
}

// decideSnapshot is the JSON payload cached for the decision replay window.
// Bucket rows are [remaining_mt, capacity_mt, rate_mtps, layer, name, policy].
type decideSnapshot struct {
	FP         string       `json:"fp"`
	Status     string       `json:"status"`
	NowMs      int64        `json:"now_ms"`
	CostMt     int64        `json:"cost_mt"`
	WaitMs     int64        `json:"wait_ms"`
	LimitedIdx int64        `json:"limited_idx"`
	BucketRows [][6]any     `json:"buckets"`
	Raw        []BucketView `json:"-"`
}

func parseSnapshot(v interface{}) (*decideSnapshot, error) {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		return nil, fmt.Errorf("unexpected snapshot type %T", v)
	}
	var snap decideSnapshot
	if err := json.Unmarshal([]byte(s), &snap); err != nil {
		return nil, err
	}
	for _, row := range snap.BucketRows {
		if len(row) != 6 {
			return nil, errors.New("malformed snapshot bucket row")
		}
		rem, err := anyToInt64(row[0])
		if err != nil {
			return nil, err
		}
		capM, err := anyToInt64(row[1])
		if err != nil {
			return nil, err
		}
		rate, err := anyToInt64(row[2])
		if err != nil {
			return nil, err
		}
		snap.Raw = append(snap.Raw, BucketView{
			Remaining:  rem,
			Capacity:   capM,
			RefillRate: rate,
			Layer:      fmt.Sprint(row[3]),
			Name:       fmt.Sprint(row[4]),
			PolicyID:   policyToInt64(row[5]),
		})
	}
	return &snap, nil
}

func anyToInt64(v any) (int64, error) {
	switch x := v.(type) {
	case float64:
		return int64(x), nil
	case int64:
		return x, nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	}
	return 0, fmt.Errorf("unexpected numeric type %T", v)
}

func policyToInt64(v any) int64 {
	n, err := anyToInt64(v)
	if err != nil {
		return 0
	}
	return n
}

// Decide performs the atomic all-or-nothing multi-bucket check.
func (e *Engine) Decide(ctx context.Context, req CheckRequest) (Decision, error) {
	if err := validateToken("tenant", req.Tenant); err != nil {
		return Decision{}, err
	}
	if err := validateToken("api", req.API); err != nil {
		return Decision{}, err
	}
	if req.User != "" {
		if err := validateToken("user", req.User); err != nil {
			return Decision{}, err
		}
	}
	if len(req.Buckets) == 0 {
		return Decision{}, errors.New("ratelimit: no buckets resolved")
	}
	cost := req.Cost
	if cost == 0 {
		cost = 1000
	}
	for _, b := range req.Buckets {
		if cost > b.Capacity {
			return Decision{}, fmt.Errorf("%w: layer=%s capacity=%d cost=%d",
				ErrCostTooLarge, b.Layer, b.Capacity, cost)
		}
		if b.Capacity > MaxCapacityMilliTokens || b.RefillRate > MaxRateMilliTPS {
			return Decision{}, fmt.Errorf("ratelimit: bucket parameters exceed safety caps")
		}
		if b.IdleTTL <= 0 {
			return Decision{}, errors.New("ratelimit: bucket idle ttl required")
		}
	}

	keys := make([]string, len(req.Buckets))
	args := make([]interface{}, 0, 6+len(req.Buckets)*8)
	args = append(args, cost, "", req.Fingerprint,
		e.idemAllowTTL.Milliseconds(), e.idemDenyTTL.Milliseconds(), len(req.Buckets))

	idemK := ""
	if req.IdempotencyKey != "" {
		if err := validateToken("idempotency_key", req.IdempotencyKey); err != nil {
			return Decision{}, err
		}
		if len(req.Fingerprint) != 64 || !isLowerHex(req.Fingerprint) {
			return Decision{}, errors.New("ratelimit: fingerprint must be sha256 hex (64 chars)")
		}
		idemK = idemKey(req.Tenant, req.IdempotencyKey)
		args[1] = idemK
	}

	for i, b := range req.Buckets {
		keys[i] = bucketKey(req.Tenant, b)
		idle := b.IdleTTL.Milliseconds()
		args = append(args, keys[i], b.Capacity, b.RefillRate, b.Version, idle)
	}
	// Tail block: layer/name/policy id per bucket.
	for _, b := range req.Buckets {
		args = append(args, b.Layer, b.Name, b.PolicyID)
	}
	cctx, cancel := context.WithTimeout(ctx, e.decisionBudget)
	defer cancel()

	res, err := e.decideScript.Run(cctx, e.rdb, keys, args...).Result()
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	vals, ok := res.([]interface{})
	if !ok || len(vals) < 3 {
		return Decision{}, fmt.Errorf("%w: malformed script reply", ErrUnavailable)
	}

	status, _ := vals[0].(string)
	nowMs, _ := toInt64(vals[1])
	now := time.UnixMilli(nowMs).UTC()

	switch status {
	case "ALLOW", "DENY":
		d := Decision{Now: now}
		if status == "ALLOW" {
			d.Allowed = true
		} else {
			wait, _ := toInt64(vals[3])
			idx, _ := toInt64(vals[4])
			if wait == 0 {
				d.NeverRetry = true
			} else {
				d.Wait = time.Duration(wait) * time.Millisecond
			}
			_ = idx // layer resolved from bucket rows below
		}
		_, buckets, _, perr := parseBucketRows(vals, indexOfRows(status))
		if perr != nil {
			return Decision{}, fmt.Errorf("%w: %v", ErrUnavailable, perr)
		}
		attachPolicyIDs(&buckets, req.Buckets)
		d.Buckets = buckets
		if !d.Allowed {
			for _, bv := range buckets {
				if bv.Remaining < cost {
					d.LimitedLayer = bv.Layer
					d.LimitedName = bv.Name
					break
				}
			}
		}
		return d, nil

	case "IDEM":
		// vals[3] is the cached decision snapshot JSON; fp already matched
		// atomically inside the script.
		snap, err := parseSnapshot(vals[3])
		if err != nil {
			return Decision{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		d := Decision{
			Now:      now,
			Replayed: true,
			Allowed:  snap.Status == "ALLOW",
		}
		if !d.Allowed {
			if snap.WaitMs == 0 {
				d.NeverRetry = true
			} else {
				d.Wait = time.Duration(snap.WaitMs) * time.Millisecond
			}
		}
		d.Buckets = snap.Raw
		if !d.Allowed && snap.LimitedIdx >= 1 && int(snap.LimitedIdx) <= len(d.Buckets) {
			b := d.Buckets[snap.LimitedIdx-1]
			d.LimitedLayer = b.Layer
			d.LimitedName = b.Name
		}
		return d, nil

	case "CONFLICT":
		return Decision{}, ErrConflict

	default:
		return Decision{}, fmt.Errorf("%w: unknown status %q", ErrUnavailable, status)
	}
}

// indexOfRows gives the starting index of the n_bucket row block.
func indexOfRows(status string) int {
	if status == "DENY" {
		return 5 // status,now,cost,wait,limited_idx
	}
	return 3 // status,now,cost
}

// DedupResult of a business dedup check.
type DedupResult struct {
	First     bool
	Now       time.Time
	Duplicate bool
}

// BusinessDedup claims a business dedup key for its bounded window.
// Independent from decision idempotency (namespace, TTL, script).
func (e *Engine) BusinessDedup(ctx context.Context, tenant, key, fingerprint string, ttl time.Duration) (DedupResult, error) {
	if err := validateToken("tenant", tenant); err != nil {
		return DedupResult{}, err
	}
	if err := validateToken("dedup_key", key); err != nil {
		return DedupResult{}, err
	}
	if ttl <= 0 {
		ttl = e.dedupTTL
	}
	k := DedupKey(tenant, key)
	cctx, cancel := context.WithTimeout(ctx, e.decisionBudget)
	defer cancel()
	res, err := e.dedupScript.Run(cctx, e.rdb, []string{k}, k, fingerprint, ttl.Milliseconds()).Result()
	if err != nil {
		return DedupResult{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	vals, ok := res.([]interface{})
	if !ok || len(vals) != 2 {
		return DedupResult{}, fmt.Errorf("%w: malformed dedup reply", ErrUnavailable)
	}
	status, _ := vals[0].(string)
	nowMs, _ := toInt64(vals[1])
	switch status {
	case "OK":
		return DedupResult{First: true, Now: time.UnixMilli(nowMs).UTC()}, nil
	case "DUPLICATE":
		return DedupResult{Duplicate: true, Now: time.UnixMilli(nowMs).UTC()}, nil
	case "CONFLICT":
		return DedupResult{}, ErrConflict
	default:
		return DedupResult{}, fmt.Errorf("%w: unknown dedup status %q", ErrUnavailable, status)
	}
}

func attachPolicyIDs(buckets *[]BucketView, specs []BucketSpec) {
	for i := range *buckets {
		for _, s := range specs {
			if s.Layer == (*buckets)[i].Layer && s.Name == (*buckets)[i].Name {
				(*buckets)[i].PolicyID = s.PolicyID
			}
		}
	}
}
