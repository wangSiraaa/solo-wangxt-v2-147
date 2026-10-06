package policy

import (
	"fmt"

	"ratelimit-platform/internal/ratelimit"
)

// Resolve builds the bucket list for a request against a snapshot.
//
// The API bucket is always present (keyed by api id). The tenant bucket is
// always present (subject = tenant). The user bucket is present only when a
// non-empty user id is supplied AND a user-layer policy resolves; a user
// without a policy is simply not rate limited at the user layer.
//
// Missing tenant/api policy, unknown endpoint, or a disabled endpoint is an
// error; policy gaps there must be explicit.
func Resolve(snap *Snapshot, tenant, user, api string) (*Resolved, error) {
	if snap == nil {
		return nil, fmt.Errorf("%w: policy snapshot not loaded", ErrNoPolicy)
	}
	ep, ok := snap.Endpoints[api]
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q", ErrNotFound, api)
	}
	if ep.Disabled {
		return nil, fmt.Errorf("%w: endpoint %q", ErrEndpointDisabled, api)
	}

	r := &Resolved{Endpoint: ep}

	add := func(layer, subject string) error {
		b, ok := snap.binding(layer, subject)
		if !ok {
			if layer == ratelimit.LayerUser {
				return nil // user layer is optional per principal
			}
			return fmt.Errorf("%w: layer=%s subject=%q", ErrNoPolicy, layer, subject)
		}
		p, ok := snap.Policies[b.PolicyID]
		if !ok {
			return fmt.Errorf("%w: policy id=%d", ErrNotFound, b.PolicyID)
		}
		if p.Disabled {
			if layer == ratelimit.LayerUser {
				return nil
			}
			return fmt.Errorf("%w: layer=%s policy=%q disabled", ErrNoPolicy, layer, p.Name)
		}
		r.Buckets = append(r.Buckets, ResolvedBucket{
			Layer:   layer,
			Subject: subject,
			Policy:  p,
		})
		return nil
	}

	if err := add(ratelimit.LayerTenant, tenant); err != nil {
		return nil, err
	}
	if user != "" {
		if err := add(ratelimit.LayerUser, user); err != nil {
			return nil, err
		}
	}
	if err := add(ratelimit.LayerAPI, api); err != nil {
		return nil, err
	}
	return r, nil
}
