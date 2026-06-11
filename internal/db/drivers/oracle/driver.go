package oracle

import (
	"context"
	"database/sql"
	"fmt"

	"dbbridge/internal/db"

	_ "github.com/sijms/go-ora/v2"
)

func init() {
	db.Register("oracle", &oracleDriver{})
}

type oracleDriver struct{}

func (d *oracleDriver) Open(ctx context.Context, dsn string, maxConns int) (db.Pool, error) {
	dbConn, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open oracle connection: %w", err)
	}

	if maxConns > 0 {
		dbConn.SetMaxOpenConns(maxConns)
		dbConn.SetMaxIdleConns(maxConns)
	}

	// Verify connection
	if err := dbConn.PingContext(ctx); err != nil {
		_ = dbConn.Close()
		return nil, fmt.Errorf("failed to ping oracle: %w", err)
	}

	return &oraclePool{db: dbConn}, nil
}

type oraclePool struct {
	db *sql.DB
}

func (p *oraclePool) Exec(ctx context.Context, sql string) (db.RowStream, error) {
	rows, err := p.db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &oracleRowStream{rows: rows}, nil
}

func (p *oraclePool) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

func (p *oraclePool) Close() error {
	return p.db.Close()
}

type oracleRowStream struct {
	rows *sql.Rows
}

func (s *oracleRowStream) Columns() ([]string, error) {
	return s.rows.Columns()
}

func (s *oracleRowStream) Next() bool {
	return s.rows.Next()
}

func (s *oracleRowStream) Scan(dest ...any) error {
	return s.rows.Scan(dest...)
}

func (s *oracleRowStream) Err() error {
	return s.rows.Err()
}

func (s *oracleRowStream) Close() error {
	return s.rows.Close()
}
