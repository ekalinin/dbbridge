// Package e2e_test contains end-to-end tests that start an in-process HTTP
// server backed by an in-memory MetaStore, local-FS ResultStore, and a
// fake database driver — no external services required.
package e2e_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/storage/backends/fs"
	"github.com/ekalinin/dbbridge/internal/transport/rest"
)

// process-wide temp dir for FS result storage — shared across all tests.
var globalResultsDir string

// TestMain registers global singletons once for the whole test binary.
// It sets up fake database drivers and a temporary directory for file storage
// that will be used across all end-to-end tests.
func TestMain(m *testing.M) {
	// Register fake DB drivers.
	// "postgres" is used because the config validator only accepts known engine names
	// and the real postgres driver is NOT blank-imported in this test binary.
	db.Register("postgres", fakeDriver{})
	db.Register("slowdb", slowDriver{})

	// Register FS storage once (storage.Register panics on duplicate).
	tmpDir, err := os.MkdirTemp("", "dbbridge-e2e-results-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	globalResultsDir = tmpDir

	fsStore, err := fs.NewFSResultStore(tmpDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create FS store: %v\n", err)
		os.Exit(1)
	}
	storage.Register("fs", fsStore)

	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// testHarness wraps the in-process HTTP test server and its URL.
// It provides a self-contained environment for running end-to-end tests
// with all components (MetaStore, QueryManager, etc.) isolated per test.
type testHarness struct {
	baseURL string
}

// newHarness creates a fresh in-process server for one test.
// Each call gets its own MetaStore and QueryManager so tests are isolated.
// It initializes all necessary components including config, metastore, query manager,
// and REST server, and returns a test harness with the server's base URL.
func newHarness(t *testing.T) *testHarness {
	t.Helper()

	cfgContent := fmt.Sprintf(`
instance:
  id: e2e-test
  metastore: memory
  default_storage: fs
  heartbeat_ttl: 1s
server:
  rest_addr: ":0"
  grpc_addr: ":0"
defaults:
  result_ttl: 1h
storage:
  fs:
    root: %s
databases:
  - id: testdb
    engine: postgres
    dsn: "postgres://fake:fake@localhost/fake"
    display_name: "Test DB"
    max_conns: 2
`, globalResultsDir)

	cfgFile, err := os.CreateTemp(t.TempDir(), "dbbridge-*.yaml")
	if err != nil {
		t.Fatalf("create config file: %v", err)
	}
	if _, err := cfgFile.WriteString(cfgContent); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	cfgFile.Close()

	cfgMgr, err := config.NewManager(cfgFile.Name())
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}

	metaStore := state.NewMemoryMetaStore()
	t.Cleanup(func() { metaStore.Close() })

	lm := lifecycle.NewManager()

	qm, err := manager.NewQueryManager(cfgMgr, metaStore)
	if err != nil {
		t.Fatalf("manager.NewQueryManager: %v", err)
	}
	t.Cleanup(func() { qm.Close() })

	svc := service.NewQueryService(qm, lm)
	srv := rest.NewServer(svc)

	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)

	return &testHarness{baseURL: hs.URL}
}

// ── Fake drivers ─────────────────────────────────────────────────────────────

// fakeDriver opens pools that return two static rows for any SQL.
// It's used for testing without requiring a real database connection.
type fakeDriver struct{}

// Open creates a new fake database pool.
func (fakeDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return fakePool{}, nil
}

// fakePool implements db.Pool interface for testing purposes.
// It returns predefined static data for any query.
type fakePool struct{}

// Exec executes a query and returns a fake row stream with static data.
// The returned stream contains two rows with columns "id" and "name".
func (fakePool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return &fakeRowStream{
		cols: []string{"id", "name"},
		rows: [][]any{
			{int64(1), "alice"},
			{int64(2), "bob"},
		},
		pos: -1,
	}, nil
}

// Ping simulates a database connectivity check.
func (fakePool) Ping(_ context.Context) error { return nil }

// Stat returns empty pool statistics.
func (fakePool) Stat() db.PoolStat { return db.PoolStat{} }

// Close closes the pool.
func (fakePool) Close() error { return nil }

// fakeRowStream implements db.RowStream for testing purposes.
// It provides a static set of rows for testing query execution.
type fakeRowStream struct {
	cols []string
	rows [][]any
	pos  int
}

// Columns returns the column names of the result set.
func (s *fakeRowStream) Columns() ([]string, error) { return s.cols, nil }

// Next advances to the next row in the result set.
// Returns true if there are more rows, false otherwise.
func (s *fakeRowStream) Next() bool {
	s.pos++
	return s.pos < len(s.rows)
}

// Scan copies the values of the current row into the provided destinations.
func (s *fakeRowStream) Scan(dest ...any) error {
	row := s.rows[s.pos]
	for i, d := range dest {
		if p, ok := d.(*any); ok {
			*p = row[i]
		}
	}
	return nil
}

// Err returns any error that occurred during iteration.
func (s *fakeRowStream) Err() error { return nil }

// Close closes the row stream.
func (s *fakeRowStream) Close() error { return nil }

// slowDriver blocks Exec until its context is canceled (for cancel-query tests).
// It's used to test query cancellation functionality.
type slowDriver struct{}

// Open creates a new slow database pool.
func (slowDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return slowPool{}, nil
}

// slowPool implements db.Pool interface for testing query cancellation.
// Its Exec method blocks until the context is canceled.
type slowPool struct{}

// Exec blocks until the context is canceled, then returns the context error.
// This is used to test query cancellation functionality.
func (slowPool) Exec(ctx context.Context, _ string) (db.RowStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Ping simulates a database connectivity check.
func (slowPool) Ping(_ context.Context) error { return nil }

// Stat returns empty pool statistics.
func (slowPool) Stat() db.PoolStat { return db.PoolStat{} }

// Close closes the pool.
func (slowPool) Close() error { return nil }
