// Package policy persists rate-limit policy in PostgreSQL and exposes an
// in-memory read model used by the decision hot path.
//
// Resolution order per layer (tenant / user / api):
//
//	specific binding (subject set) > layer default policy
//
// Every layer is independent: a missing user binding does not disable the
// tenant bucket; it falls back to the user-level default (if any).
package policy

import (
	"errors"
)

// FailPolicy decides behavior when Redis cannot answer a decision.
type FailPolicy string

const (
	// FailOpen permits the request when the limiter is unavailable
	// (appropriate for low-risk, availability-sensitive APIs).
	FailOpen FailPolicy = "open"
	// FailClosed rejects the request when the limiter is unavailable
	// (appropriate for high-risk APIs: payments, auth, writes with cost).
	FailClosed FailPolicy = "closed"
)

// Policy is a reusable token-bucket definition. Rate is stored as integer
// millitokens per second (1000 = 1 token/s) to avoid float drift.
type Policy struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	CapacityMT  int64  `json:"capacity_mt"`
	RefillMTPS  int64  `json:"refill_mtps"`
	Description string `json:"description"`
	Version     int64  `json:"version"`
	Disabled    bool   `json:"disabled"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

// Endpoint is a registered API and its Redis-failure risk posture.
type Endpoint struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Risk       string     `json:"risk"` // informational label
	FailPolicy FailPolicy `json:"fail_policy"`
	Disabled   bool       `json:"disabled"`
	UpdatedAt  string     `json:"updated_at,omitempty"`
}

// Binding attaches a policy to a layer subject. Exactly one of TenantID /
// UserID / APIID is set; empty subject means "layer default".
type Binding struct {
	ID         int64  `json:"id"`
	Layer      string `json:"layer"`
	TenantID   string `json:"tenant_id,omitempty"`
	UserID     string `json:"user_id,omitempty"`
	APIID      string `json:"api_id,omitempty"`
	PolicyID   int64  `json:"policy_id"`
	PolicyName string `json:"policy_name,omitempty"`
}

// Snapshot is an immutable read model of all policy data at one version.
type Snapshot struct {
	Version   uint64
	Policies  map[int64]Policy
	Endpoints map[string]Endpoint
	Bindings  map[string]Binding // key: bindingKey(layer, subject)
	// Defaults keyed by layer (subject "").
}

func (s *Snapshot) binding(layer, subject string) (Binding, bool) {
	if subject != "" {
		if b, ok := s.Bindings[bindingKey(layer, subject)]; ok {
			return b, true
		}
	}
	if b, ok := s.Bindings[bindingKey(layer, "")]; ok {
		return b, true
	}
	return Binding{}, false
}

// ResolvedBucket is one layer policy resolved for a concrete request.
type ResolvedBucket struct {
	Layer   string
	Subject string
	Policy  Policy
}

// Resolved is the full per-request resolution.
type Resolved struct {
	Buckets  []ResolvedBucket
	Endpoint Endpoint
}

var (
	// ErrNotFound is returned for unknown policy/endpoint/binding.
	ErrNotFound = errors.New("policy: not found")
	// ErrEndpointDisabled means the API is administratively off.
	ErrEndpointDisabled = errors.New("policy: endpoint disabled")
	// ErrNoPolicy means at least one mandatory layer has no policy chain.
	ErrNoPolicy = errors.New("policy: no policy for layer")
)

// Resolver returns policy data for a request.
type Resolver interface {
	Snapshot() *Snapshot
}

func bindingKey(layer, subject string) string {
	return layer + "|" + subject
}

// IsDefault reports whether the binding is a layer-default (empty subject).
func (b Binding) IsDefault() bool {
	return b.TenantID == "" && b.UserID == "" && b.APIID == ""
}

// Subject returns the concrete subject identifier for a binding.
func (b Binding) Subject() string {
	switch b.Layer {
	case "tenant":
		return b.TenantID
	case "user":
		return b.UserID
	case "api":
		return b.APIID
	}
	return ""
}
