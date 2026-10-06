package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"ratelimit-platform/internal/policy"
)

// AdminStore abstracts the mutable backend without forcing every test to
// supply a CachedStore.
type AdminStore interface {
	policy.Store
}

// AdminServer holds admin-only dependencies.
type AdminServer struct {
	Store   AdminStore
	Refresh func() error // nil if admin runs without a cache layer
}

// RegisterAdmin mounts admin routes on r.
func (a *AdminServer) RegisterAdmin(r *gin.Engine) {
	g := r.Group("/admin/v1")
	g.GET("/policies", a.listPolicies)
	g.POST("/policies", a.createPolicy)
	g.PUT("/policies/:id", a.updatePolicy)
	g.DELETE("/policies/:id", a.deletePolicy)

	g.GET("/endpoints", a.listEndpoints)
	g.PUT("/endpoints/:id", a.upsertEndpoint)
	g.DELETE("/endpoints/:id", a.deleteEndpoint)

	g.GET("/bindings", a.listBindings)
	g.PUT("/bindings", a.setBinding)
	g.DELETE("/bindings", a.deleteBinding)

	g.POST("/cache/refresh", a.refreshCache)
}

type policyInput struct {
	Name       string `json:"name"`
	CapacityMT int64  `json:"capacity_mt"`
	RefillMTPS int64  `json:"refill_mtps"`
	// Convenience fields with up to 3 decimal places (tokens), converted to
	// integer millitokens without float drift.
	CapacityTokens *json.Number `json:"capacity_tokens,omitempty"`
	RefillPerSec   *json.Number `json:"refill_per_sec,omitempty"`
	Description    string       `json:"description"`
	Disabled       bool         `json:"disabled"`
}

func decodePolicyInput(in policyInput) (policy.Policy, error) {
	p := policy.Policy{
		Name:        in.Name,
		CapacityMT:  in.CapacityMT,
		RefillMTPS:  in.RefillMTPS,
		Description: in.Description,
		Disabled:    in.Disabled,
	}
	if in.CapacityTokens != nil {
		v, err := parseDecimal3(string(*in.CapacityTokens), 1000)
		if err != nil {
			return p, err
		}
		p.CapacityMT = v
	}
	if in.RefillPerSec != nil {
		v, err := parseDecimal3(string(*in.RefillPerSec), 1000)
		if err != nil {
			return p, err
		}
		p.RefillMTPS = v
	}
	if p.Name == "" {
		return p, errors.New("name is required")
	}
	if p.CapacityMT < 1 || p.CapacityMT > 1_000_000_000 {
		return p, errors.New("capacity_mt must be in [1, 1000000000]")
	}
	if p.RefillMTPS < 0 || p.RefillMTPS > 1_000_000_000 {
		return p, errors.New("refill_mtps must be in [0, 1000000000]")
	}
	return p, nil
}

// parseDecimal3 parses a decimal string allowing at most `scale`-1 fractional
// digits and returns the scaled integer.
func parseDecimal3(s string, scale int64) (int64, error) {
	dot := -1
	for i, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r == '.' && dot < 0:
			dot = i
		case r == '-' && i == 0:
		default:
			return 0, errors.New("invalid decimal: " + s)
		}
	}
	intPart, frac := s, ""
	if dot >= 0 {
		intPart, frac = s[:dot], s[dot+1:]
	}
	if len(frac) > 3 {
		return 0, errors.New("at most 3 decimal places supported")
	}
	whole, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0, errors.New("invalid decimal: " + s)
	}
	for len(frac) < 3 {
		frac += "0"
	}
	part, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, errors.New("invalid decimal: " + s)
	}
	return whole*scale + part, nil
}

func (a *AdminServer) listPolicies(c *gin.Context) {
	out, err := a.Store.ListPolicies(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"policies": out})
}

func (a *AdminServer) createPolicy(c *gin.Context) {
	var in policyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	p, err := decodePolicyInput(in)
	if err != nil {
		badRequest(c, err.Error())
		return
	}
	out, err := a.Store.CreatePolicy(c.Request.Context(), p)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, out)
}

func (a *AdminServer) updatePolicy(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		badRequest(c, "id must be integer")
		return
	}
	var in policyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	p, err := decodePolicyInput(in)
	if err != nil {
		badRequest(c, err.Error())
		return
	}
	p.ID = id
	out, err := a.Store.UpdatePolicy(c.Request.Context(), p)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, policy.ErrNotFound) {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, out)
}

func (a *AdminServer) deletePolicy(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		badRequest(c, "id must be integer")
		return
	}
	if err := a.Store.DeletePolicy(c.Request.Context(), id); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, policy.ErrNotFound) {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

// --- endpoints -------------------------------------------------------------

type endpointInput struct {
	Name       string `json:"name"`
	Risk       string `json:"risk"`
	FailPolicy string `json:"fail_policy"`
	Disabled   bool   `json:"disabled"`
}

func (a *AdminServer) listEndpoints(c *gin.Context) {
	out, err := a.Store.ListEndpoints(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"endpoints": out})
}

func (a *AdminServer) upsertEndpoint(c *gin.Context) {
	var in endpointInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	fp := policy.FailPolicy(in.FailPolicy)
	if fp != policy.FailOpen && fp != policy.FailClosed {
		badRequest(c, "fail_policy must be 'open' or 'closed'")
		return
	}
	e := policy.Endpoint{
		ID:         c.Param("id"),
		Name:       in.Name,
		Risk:       in.Risk,
		FailPolicy: fp,
		Disabled:   in.Disabled,
	}
	out, err := a.Store.UpsertEndpoint(c.Request.Context(), e)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, out)
}

func (a *AdminServer) deleteEndpoint(c *gin.Context) {
	if err := a.Store.DeleteEndpoint(c.Request.Context(), c.Param("id")); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, policy.ErrNotFound) {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

// --- bindings --------------------------------------------------------------

type bindingInput struct {
	Layer    string `json:"layer" binding:"required"`
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
	APIID    string `json:"api_id"`
	PolicyID int64  `json:"policy_id" binding:"required"`
}

func (in bindingInput) toBinding() (policy.Binding, error) {
	b := policy.Binding{
		Layer:    in.Layer,
		TenantID: in.TenantID,
		UserID:   in.UserID,
		APIID:    in.APIID,
		PolicyID: in.PolicyID,
	}
	var subject string
	switch in.Layer {
	case "tenant":
		subject = in.TenantID
	case "user":
		subject = in.UserID
	case "api":
		subject = in.APIID
	default:
		return b, errors.New("layer must be tenant|user|api")
	}
	// Only the matching subject column is permitted; the others must be empty.
	if (in.Layer != "tenant" && in.TenantID != "") ||
		(in.Layer != "user" && in.UserID != "") ||
		(in.Layer != "api" && in.APIID != "") {
		return b, errors.New("subject fields other than the layer's own must be empty")
	}
	_ = subject // "" is valid here: it encodes the layer-default binding.
	return b, nil
}

func (a *AdminServer) listBindings(c *gin.Context) {
	out, err := a.Store.ListBindings(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"bindings": out})
}

func (a *AdminServer) setBinding(c *gin.Context) {
	var in bindingInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	b, err := in.toBinding()
	if err != nil {
		badRequest(c, err.Error())
		return
	}
	out, err := a.Store.SetBinding(c.Request.Context(), b)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, out)
}

func (a *AdminServer) deleteBinding(c *gin.Context) {
	q := c.Request.URL.Query()
	layer := q.Get("layer")
	if layer == "" {
		badRequest(c, "layer query parameter is required")
		return
	}
	if err := a.Store.DeleteBinding(c.Request.Context(),
		layer, q.Get("tenant_id"), q.Get("user_id"), q.Get("api_id")); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, policy.ErrNotFound) {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (a *AdminServer) refreshCache(c *gin.Context) {
	if a.Refresh == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "cache refresh unavailable in this deployment"})
		return
	}
	if err := a.Refresh(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "refreshed"})
}
