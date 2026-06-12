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

	"dbbridge/internal/config"
	"dbbridge/internal/core/manager"
	"dbbridge/internal/core/service"
	"dbbridge/internal/db"
	"dbbridge/internal/lifecycle"
	"dbbridge/internal/state"
	"dbbridge/internal/storage"
	"dbbridge/internal/storage/backends/fs"
	"dbbridge/internal/transport/rest"
)

// process-wide temp dir for FS result storage — shared across all tests.
var globalResultsDir string

// TestMain registers global singletons once for the whole test binary.
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
type testHarness struct {
	baseURL string
}

// newHarness creates a fresh in-process server for one test.
// Each call gets its own MetaStore and QueryManager so tests are isolated.
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
type fakeDriver struct{}

func (fakeDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return fakePool{}, nil
}

type fakePool struct{}

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

func (fakePool) Ping(_ context.Context) error { return nil }
func (fakePool) Stat() db.PoolStat            { return db.PoolStat{} }
func (fakePool) Close() error                 { return nil }

type fakeRowStream struct {
	cols []string
	rows [][]any
	pos  int
}

func (s *fakeRowStream) Columns() ([]string, error) { return s.cols, nil }

func (s *fakeRowStream) Next() bool {
	s.pos++
	return s.pos < len(s.rows)
}

func (s *fakeRowStream) Scan(dest ...any) error {
	row := s.rows[s.pos]
	for i, d := range dest {
		if p, ok := d.(*any); ok {
			*p = row[i]
		}
	}
	return nil
}

func (s *fakeRowStream) Err() error { return nil }
func (s *fakeRowStream) Close() error { return nil }

// slowDriver blocks Exec until its context is canceled (for cancel-query tests).
type slowDriver struct{}

func (slowDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return slowPool{}, nil
}

type slowPool struct{}

func (slowPool) Exec(ctx context.Context, _ string) (db.RowStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (slowPool) Ping(_ context.Context) error { return nil }
func (slowPool) Stat() db.PoolStat            { return db.PoolStat{} }
func (slowPool) Close() error                 { return nil }
