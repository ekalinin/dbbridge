package storage

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ekalinin/dbbridge/internal/core/domain"
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

// FormatChecker is implemented by a backend that cannot hold every result
// format losslessly. A store that does not implement it is assumed to return
// exactly the bytes it was given.
type FormatChecker interface {
	// SupportsFormat reports whether a result in this format survives a write
	// followed by a read byte for byte.
	SupportsFormat(format string) bool
}

// SupportsFormat reports whether store can hold format without altering it.
// The pair is checked at submission time: the alternative is a query that runs
// to completion and leaves behind a result file its own checksum no longer
// matches.
func SupportsFormat(store ResultStore, format string) bool {
	fc, ok := store.(FormatChecker)
	if !ok {
		return true
	}
	return fc.SupportsFormat(format)
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
