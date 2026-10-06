package policy

import (
	"context"
	"log"
)

// Seed installs a coherent demo configuration. It is idempotent enough for
// local development: memory backend always seeds; PostgreSQL seeds only when
// no endpoints exist yet.
//
// Demonstrates the three layers and BOTH redis-failure postures:
//   - api.login        high risk   -> fail closed
//   - api.pay          high risk   -> fail closed
//   - api.search       low risk    -> fail open
//   - api.docs         low risk    -> fail open
func Seed(ctx context.Context, s Store) error {
	pDefs := []Policy{
		{Name: "tenant-default", CapacityMT: 100 * 1000, RefillMTPS: 10 * 1000, Description: "100 burst, 10 req/s per tenant"},
		{Name: "user-default", CapacityMT: 20 * 1000, RefillMTPS: 2 * 1000, Description: "20 burst, 2 req/s per user"},
		{Name: "api-login", CapacityMT: 5 * 1000, RefillMTPS: 100, Description: "5 burst, 0.1 req/s login"},
		{Name: "api-pay", CapacityMT: 10 * 1000, RefillMTPS: 1 * 1000, Description: "10 burst, 1 req/s pay"},
		{Name: "api-search", CapacityMT: 50 * 1000, RefillMTPS: 20 * 1000, Description: "50 burst, 20 req/s search"},
		{Name: "api-docs", CapacityMT: 500 * 1000, RefillMTPS: 100 * 1000, Description: "500 burst, 100 req/s docs"},
	}
	policyID := map[string]int64{}
	for _, p := range pDefs {
		got, err := s.CreatePolicy(ctx, p)
		if err != nil {
			return err
		}
		policyID[p.Name] = got.ID
	}

	eps := []Endpoint{
		{ID: "login", Name: "Login", Risk: "high", FailPolicy: FailClosed},
		{ID: "pay", Name: "Payment", Risk: "high", FailPolicy: FailClosed},
		{ID: "search", Name: "Search", Risk: "low", FailPolicy: FailOpen},
		{ID: "docs", Name: "Documentation", Risk: "low", FailPolicy: FailOpen},
	}
	for _, e := range eps {
		if _, err := s.UpsertEndpoint(ctx, e); err != nil {
			return err
		}
	}

	bindings := []Binding{
		{Layer: "tenant", PolicyID: policyID["tenant-default"]},
		{Layer: "user", PolicyID: policyID["user-default"]},
		{Layer: "api", APIID: "login", PolicyID: policyID["api-login"]},
		{Layer: "api", APIID: "pay", PolicyID: policyID["api-pay"]},
		{Layer: "api", APIID: "search", PolicyID: policyID["api-search"]},
		{Layer: "api", APIID: "docs", PolicyID: policyID["api-docs"]},
	}
	for _, b := range bindings {
		if _, err := s.SetBinding(ctx, b); err != nil {
			return err
		}
	}
	log.Printf("policy: seeded %d policies, %d endpoints, %d bindings",
		len(pDefs), len(eps), len(bindings))
	return nil
}

// SeedIfEmpty seeds a PostgreSQL backend only when uninitialized.
func SeedIfEmpty(ctx context.Context, s Store) error {
	eps, err := s.ListEndpoints(ctx)
	if err != nil {
		return err
	}
	if len(eps) > 0 {
		return nil
	}
	return Seed(ctx, s)
}
