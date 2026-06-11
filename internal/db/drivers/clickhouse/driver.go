package clickhouse

import (
	"context"
	"database/sql"
	"fmt"

	"dbbridge/internal/db"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

func init() {
	db.Register("clickhouse", &clickhouseDriver{})
}

type clickhouseDriver struct{}

func (d *clickhouseDriver) Open(ctx context.Context, dsn string, maxConns int) (db.Pool, error) {
	dbConn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	if maxConns > 0 {
		dbConn.SetMaxOpenConns(maxConns)
		dbConn.SetMaxIdleConns(maxConns)
	}

	// Verify connection
	if err := dbConn.PingContext(ctx); err != nil {
		_ = dbConn.Close()
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &clickhousePool{db: dbConn}, nil
}

type clickhousePool struct {
	db *sql.DB
}

func (p *clickhousePool) Exec(ctx context.Context, sql string) (db.RowStream, error) {
	rows, err := p.db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &clickhouseRowStream{rows: rows}, nil
}

func (p *clickhousePool) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

func (p *clickhousePool) Close() error {
	return p.db.Close()
}

type clickhouseRowStream struct {
	rows *sql.Rows
}

func (s *clickhouseRowStream) Columns() ([]string, error) {
	return s.rows.Columns()
}

func (s *clickhouseRowStream) Next() bool {
	return s.rows.Next()
}

func (s *clickhouseRowStream) Scan(dest ...any) error {
	return s.rows.Scan(dest...)
}

func (s *clickhouseRowStream) Err() error {
	return s.rows.Err()
}

func (s *clickhouseRowStream) Close() error {
	return s.rows.Close()
}
