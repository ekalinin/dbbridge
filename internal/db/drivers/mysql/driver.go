package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"dbbridge/internal/db"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	db.Register("mysql", &mysqlDriver{})
}

type mysqlDriver struct{}

func (d *mysqlDriver) Open(ctx context.Context, dsn string, maxConns int) (db.Pool, error) {
	dbConn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	if maxConns > 0 {
		dbConn.SetMaxOpenConns(maxConns)
		dbConn.SetMaxIdleConns(maxConns)
	}

	// Verify connection
	if err := dbConn.PingContext(ctx); err != nil {
		_ = dbConn.Close()
		return nil, fmt.Errorf("failed to ping mysql: %w", err)
	}

	return &mysqlPool{db: dbConn}, nil
}

type mysqlPool struct {
	db *sql.DB
}

func (p *mysqlPool) Exec(ctx context.Context, sql string) (db.RowStream, error) {
	rows, err := p.db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &mysqlRowStream{rows: rows}, nil
}

func (p *mysqlPool) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

func (p *mysqlPool) Close() error {
	return p.db.Close()
}

type mysqlRowStream struct {
	rows *sql.Rows
}

func (s *mysqlRowStream) Columns() ([]string, error) {
	return s.rows.Columns()
}

func (s *mysqlRowStream) Next() bool {
	return s.rows.Next()
}

func (s *mysqlRowStream) Scan(dest ...any) error {
	return s.rows.Scan(dest...)
}

func (s *mysqlRowStream) Err() error {
	return s.rows.Err()
}

func (s *mysqlRowStream) Close() error {
	return s.rows.Close()
}
