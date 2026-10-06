package policy

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed migrations/001_init.sql
var migrationSQL string

// Migrate 执行建表迁移（幂等）。
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, migrationSQL)
	return err
}

// SeedDefaults 写入三层兜底策略（已存在则跳过）。
func SeedDefaults(ctx context.Context, s Store) error {
	defaults := []Policy{
		{Scope: ScopeTenant, ScopeKey: "*", Capacity: 10000, RefillPerSec: 1000, Enabled: true},
		{Scope: ScopeUser, ScopeKey: "*", Capacity: 100, RefillPerSec: 10, Enabled: true},
		{Scope: ScopeEndpoint, ScopeKey: "*", Capacity: 5000, RefillPerSec: 500, Enabled: true},
	}
	for _, p := range defaults {
		if _, err := s.Create(ctx, p); err != nil && !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return nil
}

// PgStore 是 Store 的 PostgreSQL 实现。
type PgStore struct{ db *sql.DB }

func NewPgStore(db *sql.DB) *PgStore { return &PgStore{db: db} }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

const policyCols = `id, scope, scope_key, capacity, refill_per_sec, fail_open, enabled, updated_at`

func (s *PgStore) Create(ctx context.Context, p Policy) (Policy, error) {
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO policies (scope, scope_key, capacity, refill_per_sec, fail_open, enabled)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, updated_at`,
		string(p.Scope), p.ScopeKey, p.Capacity, p.RefillPerSec, p.FailOpen, p.Enabled,
	).Scan(&p.ID, &p.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Policy{}, ErrConflict
		}
		return Policy{}, err
	}
	return p, nil
}

func (s *PgStore) Get(ctx context.Context, id int64) (Policy, error) {
	var p Policy
	err := s.db.QueryRowContext(ctx,
		`SELECT `+policyCols+` FROM policies WHERE id = $1`, id,
	).Scan(&p.ID, &p.Scope, &p.ScopeKey, &p.Capacity, &p.RefillPerSec, &p.FailOpen, &p.Enabled, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Policy{}, ErrNotFound
	}
	return p, err
}

func (s *PgStore) Update(ctx context.Context, p Policy) (Policy, error) {
	err := s.db.QueryRowContext(ctx, `
		UPDATE policies
		SET capacity = $1, refill_per_sec = $2, fail_open = $3, enabled = $4, updated_at = now()
		WHERE id = $5
		RETURNING updated_at`,
		p.Capacity, p.RefillPerSec, p.FailOpen, p.Enabled, p.ID,
	).Scan(&p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Policy{}, ErrNotFound
	}
	return p, err
}

func (s *PgStore) Delete(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM policies WHERE id = $1`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PgStore) List(ctx context.Context) ([]Policy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+policyCols+` FROM policies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		var p Policy
		if err := rows.Scan(&p.ID, &p.Scope, &p.ScopeKey, &p.Capacity, &p.RefillPerSec, &p.FailOpen, &p.Enabled, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
