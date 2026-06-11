package postgres

import (
	"context"
	"fmt"

	"dbbridge/internal/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	db.Register("postgres", &postgresDriver{})
}

type postgresDriver struct{}

func (d *postgresDriver) Open(ctx context.Context, dsn string, maxConns int) (db.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres dsn: %w", err)
	}

	if maxConns > 0 {
		config.MaxConns = int32(maxConns)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return &postgresPool{pool: pool}, nil
}

type postgresPool struct {
	pool *pgxpool.Pool
}

func (p *postgresPool) Exec(ctx context.Context, sql string) (db.RowStream, error) {
	rows, err := p.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &postgresRowStream{rows: rows}, nil
}

func (p *postgresPool) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *postgresPool) Stat() db.PoolStat {
	s := p.pool.Stat()
	return db.PoolStat{
		Open:  s.TotalConns(),
		Idle:  s.IdleConns(),
		InUse: s.AcquiredConns(),
	}
}

func (p *postgresPool) Close() error {
	p.pool.Close()
	return nil
}

type postgresRowStream struct {
	rows pgx.Rows
}

func (s *postgresRowStream) Columns() ([]string, error) {
	fields := s.rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = f.Name
	}
	return cols, nil
}

func (s *postgresRowStream) Next() bool {
	return s.rows.Next()
}

func (s *postgresRowStream) Scan(dest ...any) error {
	return s.rows.Scan(dest...)
}

func (s *postgresRowStream) Err() error {
	return s.rows.Err()
}

func (s *postgresRowStream) Close() error {
	s.rows.Close()
	return nil
}
