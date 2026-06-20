package storage

import (
	"context"
	"fmt"
	"io"
	"sync"

	"dbbridge/internal/core/domain"
)

// ResultStore defines the interface for persisting and retrieving large query execution results.
type ResultStore interface {
	// Writer opens a writer to persist a query result.
	// It returns an io.WriteCloser to stream bytes into, and the initial ResultRef.
	Writer(ctx context.Context, queryID string, format string) (io.WriteCloser, domain.ResultRef, error)

	// Reader opens a reader to fetch previously persisted query results.
	Reader(ctx context.Context, ref domain.ResultRef) (io.ReadCloser, error)

	// Stat returns metadata (e.g. SizeBytes, RowCount) about a persisted result
	// without reading its contents.
	Stat(ctx context.Context, ref domain.ResultRef) (domain.ResultRef, error)

	// Delete removes the persisted query results from the storage backend.
	Delete(ctx context.Context, ref domain.ResultRef) error
}

var (
	storesMu sync.RWMutex
	stores   = make(map[string]ResultStore)
)

// Register registers a ResultStore backend.
func Register(name string, store ResultStore) {
	storesMu.Lock()
	defer storesMu.Unlock()
	if store == nil {
		panic("storage: Register store is nil")
	}
	if _, dup := stores[name]; dup {
		panic("storage: Register called twice for store " + name)
	}
	stores[name] = store
}

// GetStore retrieves a registered ResultStore backend.
func GetStore(name string) (ResultStore, error) {
	storesMu.RLock()
	store, ok := stores[name]
	storesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("storage: unknown storage backend %q", name)
	}
	return store, nil
}
