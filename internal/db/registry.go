package db

import (
	"context"
	"fmt"
	"sync"
)

// RowStream is an iterator over query result rows, allowing streaming of massive datasets.
type RowStream interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// PoolStat holds snapshot statistics for a connection pool.
type PoolStat struct {
	Open  int32
	Idle  int32
	InUse int32
}

// Pool represents a thread-safe connection pool to a target database.
type Pool interface {
	Exec(ctx context.Context, sql string) (RowStream, error)
	Ping(ctx context.Context) error
	Stat() PoolStat
	Close() error
}

// Driver defines the connection-factory interface for a database engine.
type Driver interface {
	Open(ctx context.Context, dsn string, maxConns int) (Pool, error)
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

// Register registers a database driver.
func Register(engine string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("db: Register driver is nil")
	}
	if _, dup := drivers[engine]; dup {
		panic("db: Register called twice for driver " + engine)
	}
	drivers[engine] = driver
}

// OpenPool opens a connection pool using a registered driver.
func OpenPool(ctx context.Context, engine string, dsn string, maxConns int) (Pool, error) {
	driversMu.RLock()
	driver, ok := drivers[engine]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("db: unknown database engine %q", engine)
	}
	return driver.Open(ctx, dsn, maxConns)
}
