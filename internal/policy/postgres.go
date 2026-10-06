package policy

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// PgStore is the PostgreSQL-backed Store.
type PgStore struct {
	pool *pgxpool.Pool

	listenCtx    context.Context
	stopListener context.CancelFunc

	mu   sync.RWMutex
	subs []chan struct{}
}

// NewPgStore connects, applies migrations, and returns the store.
func NewPgStore(ctx context.Context, dsn string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	lctx, cancel := context.WithCancel(context.Background())
	s := &PgStore{pool: pool, listenCtx: lctx, stopListener: cancel}
	if err := s.migrate(ctx); err != nil {
		cancel()
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close stops the background listener (releasing its connection) and closes
// the pool. It is safe to call even when RunListener was never started.
func (s *PgStore) Close() {
	s.stopListener()
	s.pool.Close()
}

func (s *PgStore) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// Subscribe returns a channel signaled on rl_policy_changed notifications.
// The caller is expected to start one long-lived listener via RunListener.
func (s *PgStore) Subscribe() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan struct{}, 1)
	s.subs = append(s.subs, ch)
	return ch
}

func (s *PgStore) emit() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// RunListener consumes NOTIFY events until the store is closed, reconnecting
// automatically. Must be started at most once per process; it blocks (start
// it on its own goroutine). Close releases the dedicated connection.
func (s *PgStore) RunListener(_ context.Context) {
	for {
		if s.listenCtx.Err() != nil {
			return
		}
		if err := s.listenOnce(s.listenCtx); err != nil && s.listenCtx.Err() == nil {
			time.Sleep(time.Second)
		}
	}
}

func (s *PgStore) listenOnce(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN rl_policy_changed"); err != nil {
		return err
	}
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification != nil && notification.Channel == "rl_policy_changed" {
			s.emit()
		}
	}
}

// Snapshot builds the read model in one transaction.
func (s *PgStore) Snapshot() *Snapshot {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil
	}
	defer tx.Rollback(ctx)

	snap := &Snapshot{
		Policies:  map[int64]Policy{},
		Endpoints: map[string]Endpoint{},
		Bindings:  map[string]Binding{},
	}
	rows, err := tx.Query(ctx, `
		SELECT id, name, capacity_mt, refill_mtps, COALESCE(description,''),
		       version, disabled
		FROM rl_policies`)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var p Policy
		if err := rows.Scan(&p.ID, &p.Name, &p.CapacityMT, &p.RefillMTPS,
			&p.Description, &p.Version, &p.Disabled); err != nil {
			rows.Close()
			return nil
		}
		snap.Policies[p.ID] = p
	}
	rows.Close()

	erows, err := tx.Query(ctx, `
		SELECT id, COALESCE(name,''), COALESCE(risk,'normal'), fail_policy, disabled
		FROM rl_endpoints`)
	if err != nil {
		return nil
	}
	for erows.Next() {
		var e Endpoint
		if err := erows.Scan(&e.ID, &e.Name, &e.Risk, &e.FailPolicy, &e.Disabled); err != nil {
			erows.Close()
			return nil
		}
		snap.Endpoints[e.ID] = e
	}
	erows.Close()

	brows, err := tx.Query(ctx, `
		SELECT id, layer, tenant_id, user_id, api_id, policy_id
		FROM rl_bindings`)
	if err != nil {
		return nil
	}
	for brows.Next() {
		b, err := scanBinding(brows)
		if err != nil {
			brows.Close()
			return nil
		}
		if p, ok := snap.Policies[b.PolicyID]; ok {
			b.PolicyName = p.Name
		}
		snap.Bindings[bindingKey(b.Layer, b.Subject())] = b
	}
	brows.Close()

	var ver uint64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(version),1) FROM rl_policies`).Scan(&ver); err != nil {
		return nil
	}
	snap.Version = ver
	return snap
}

// --- policy CRUD -----------------------------------------------------------

// CreatePolicy inserts a policy.
func (s *PgStore) CreatePolicy(ctx context.Context, p Policy) (Policy, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO rl_policies (name, capacity_mt, refill_mtps, description, disabled)
		VALUES ($1,$2,$3,$4,$5)
		RETURNING id, name, capacity_mt, refill_mtps, COALESCE(description,''), version, disabled`,
		p.Name, p.CapacityMT, p.RefillMTPS, p.Description, p.Disabled)
	return scanPolicy(row)
}

// UpdatePolicy replaces a policy and bumps version via trigger.
func (s *PgStore) UpdatePolicy(ctx context.Context, p Policy) (Policy, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE rl_policies
		   SET name=$2, capacity_mt=$3, refill_mtps=$4, description=$5, disabled=$6
		 WHERE id=$1
		RETURNING id, name, capacity_mt, refill_mtps, COALESCE(description,''), version, disabled`,
		p.ID, p.Name, p.CapacityMT, p.RefillMTPS, p.Description, p.Disabled)
	out, err := scanPolicy(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Policy{}, ErrNotFound
	}
	return out, err
}

// DeletePolicy removes a policy.
func (s *PgStore) DeletePolicy(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM rl_policies WHERE id=$1`, id)
	if err != nil {
		return mapPgErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListPolicies returns all policies.
func (s *PgStore) ListPolicies(ctx context.Context) ([]Policy, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, capacity_mt, refill_mtps, COALESCE(description,''),
		       version, disabled
		FROM rl_policies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		var p Policy
		if err := rows.Scan(&p.ID, &p.Name, &p.CapacityMT, &p.RefillMTPS,
			&p.Description, &p.Version, &p.Disabled); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// --- endpoints -------------------------------------------------------------

// UpsertEndpoint creates or replaces an endpoint.
func (s *PgStore) UpsertEndpoint(ctx context.Context, e Endpoint) (Endpoint, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO rl_endpoints (id, name, risk, fail_policy, disabled)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (id) DO UPDATE
		   SET name=EXCLUDED.name, risk=EXCLUDED.risk,
		       fail_policy=EXCLUDED.fail_policy, disabled=EXCLUDED.disabled,
		       updated_at=now()
		RETURNING id, COALESCE(name,''), COALESCE(risk,'normal'), fail_policy, disabled`,
		e.ID, e.Name, e.Risk, e.FailPolicy, e.Disabled)
	var out Endpoint
	err := row.Scan(&out.ID, &out.Name, &out.Risk, &out.FailPolicy, &out.Disabled)
	return out, mapPgErr(err)
}

// DeleteEndpoint removes an endpoint.
func (s *PgStore) DeleteEndpoint(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM rl_endpoints WHERE id=$1`, id)
	if err != nil {
		return mapPgErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListEndpoints returns all endpoints.
func (s *PgStore) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, COALESCE(name,''), COALESCE(risk,'normal'), fail_policy, disabled
		FROM rl_endpoints ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		var e Endpoint
		if err := rows.Scan(&e.ID, &e.Name, &e.Risk, &e.FailPolicy, &e.Disabled); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- bindings --------------------------------------------------------------

// SetBinding creates or replaces a binding.
func (s *PgStore) SetBinding(ctx context.Context, b Binding) (Binding, error) {
	if err := validateBindingLayer(b.Layer); err != nil {
		return Binding{}, err
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO rl_bindings (layer, tenant_id, user_id, api_id, policy_id)
		VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), $5)
		ON CONFLICT (layer, tenant_id, user_id, api_id)
		DO UPDATE SET policy_id=EXCLUDED.policy_id
		RETURNING id, layer, tenant_id, user_id, api_id, policy_id`,
		b.Layer, b.TenantID, b.UserID, b.APIID, b.PolicyID)
	out, err := scanBinding(row)
	if err != nil {
		return Binding{}, mapPgErr(err)
	}
	return out, nil
}

// DeleteBinding removes one binding identified by layer and optional subject.
func (s *PgStore) DeleteBinding(ctx context.Context, layer, tenant, user, api string) error {
	if err := validateBindingLayer(layer); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM rl_bindings
		 WHERE layer=$1
		   AND tenant_id IS NOT DISTINCT FROM NULLIF($2,'')
		   AND user_id   IS NOT DISTINCT FROM NULLIF($3,'')
		   AND api_id    IS NOT DISTINCT FROM NULLIF($4,'')`,
		layer, tenant, user, api)
	if err != nil {
		return mapPgErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListBindings returns all bindings.
func (s *PgStore) ListBindings(ctx context.Context) ([]Binding, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, layer, tenant_id, user_id, api_id, policy_id
		FROM rl_bindings ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Binding
	for rows.Next() {
		b, err := scanBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanPolicy(r rowScanner) (Policy, error) {
	var p Policy
	err := r.Scan(&p.ID, &p.Name, &p.CapacityMT, &p.RefillMTPS,
		&p.Description, &p.Version, &p.Disabled)
	return p, mapPgErr(err)
}

func scanBinding(r rowScanner) (Binding, error) {
	var b Binding
	var tenant, user, api *string
	if err := r.Scan(&b.ID, &b.Layer, &tenant, &user, &api, &b.PolicyID); err != nil {
		return Binding{}, mapPgErr(err)
	}
	if tenant != nil {
		b.TenantID = *tenant
	}
	if user != nil {
		b.UserID = *user
	}
	if api != nil {
		b.APIID = *api
	}
	return b, nil
}

func validateBindingLayer(layer string) error {
	switch layer {
	case "tenant", "user", "api":
		return nil
	default:
		return fmt.Errorf("policy: invalid layer %q", layer)
	}
}

func mapPgErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("policy: conflict: %s", pgErr.Message)
		case "23514": // check_violation
			return fmt.Errorf("policy: invalid parameters: %s", pgErr.Message)
		case "23503": // foreign_key_violation
			return fmt.Errorf("policy: referenced policy does not exist")
		}
	}
	return err
}
