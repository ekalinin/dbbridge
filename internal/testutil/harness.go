// Package testutil provides shared, Docker-free fakes and a wiring harness for
// unit tests of the service and transport layers. It registers a fast fake DB
// driver (under engine "postgres"), a blocking "slow" driver (under engine
// "mysql"), a driver that always fails to open (under engine "clickhouse"),
// and a local-FS ResultStore — no external services required.
package testutil

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/storage/backends/fs"
)

var (
	registerOnce sync.Once
	resultsDir   string
)

// ensureRegistered registers the global fakes exactly once per test binary.
func ensureRegistered(t *testing.T) {
	t.Helper()
	registerOnce.Do(func() {
		db.Register("postgres", FakeDriver{})
		db.Register("mysql", SlowDriver{})
		db.Register("clickhouse", FailToOpenDriver{})

		dir, err := os.MkdirTemp("", "dbbridge-testutil-*")
		if err != nil {
			panic(fmt.Sprintf("testutil: mkdir temp: %v", err))
		}
		resultsDir = dir

		fsStore, err := fs.NewFSResultStore(dir)
		if err != nil {
			panic(fmt.Sprintf("testutil: fs store: %v", err))
		}
		storage.Register("fs", fsStore)
	})
}

const configTemplate = `
instance:
  id: test-instance
  metastore: memory
  default_storage: fs
  heartbeat_ttl: 200ms
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
  - id: slowdb
    engine: mysql
    dsn: "mysql://fake:fake@localhost/fake"
    display_name: "Slow DB"
    max_conns: 2
`

// NewService wires a QueryManager + QueryService backed by an in-memory MetaStore
// and the registered fakes. Returns the service and its lifecycle manager so tests
// can flip draining state. All resources are cleaned up via t.Cleanup.
func NewService(t *testing.T) (*service.QueryService, *lifecycle.Manager) {
	t.Helper()
	svc, lm, _ := NewServiceWithConfigFile(t)
	return svc, lm
}

// NewServiceWithConfigFile is like NewService but also returns the path to the
// config file backing it, for tests that need to rewrite it on disk and then
// reload - e.g. to point a database at FailToOpenDriver and exercise a failed
// config reload.
func NewServiceWithConfigFile(t *testing.T) (*service.QueryService, *lifecycle.Manager, string) {
	t.Helper()
	ensureRegistered(t)

	cfgFile, err := os.CreateTemp(t.TempDir(), "dbbridge-*.yaml")
	if err != nil {
		t.Fatalf("create config file: %v", err)
	}
	if _, err := fmt.Fprintf(cfgFile, configTemplate, resultsDir); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("close config file: %v", err)
	}

	cfgMgr, err := config.NewManager(cfgFile.Name())
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}

	ms := state.NewMemoryMetaStore()
	t.Cleanup(func() {
		if err := ms.Close(); err != nil {
			t.Errorf("close metastore: %v", err)
		}
	})

	qm, err := manager.NewQueryManager(cfgMgr, ms)
	if err != nil {
		t.Fatalf("manager.NewQueryManager: %v", err)
	}
	t.Cleanup(func() {
		if err := qm.Close(); err != nil {
			t.Errorf("close query manager: %v", err)
		}
	})

	lm := lifecycle.NewManager()
	return service.NewQueryService(qm, lm), lm, cfgFile.Name()
}

// ── Fakes ────────────────────────────────────────────────────────────────────

// FakeDriver opens pools that return two static rows for any SQL.
type FakeDriver struct{}

func (FakeDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return fakePool{}, nil
}

type fakePool struct{}

func (fakePool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return &fakeRowStream{
		cols: []string{"id", "name"},
		rows: [][]any{{int64(1), "alice"}, {int64(2), "bob"}},
		pos:  -1,
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

func (s *fakeRowStream) Err() error   { return nil }
func (s *fakeRowStream) Close() error { return nil }

// FailToOpenDriver's pools never open. The error echoes the DSN back, the way
// a real pgx/mysql/clickhouse driver's connection-refused error does, so tests
// can exercise a database that fails during config reload (rather than during
// query execution, which FakeDriver and SlowDriver already cover). Registered
// under "clickhouse" like test/e2e/suite_test.go's failToExecDriver, but the
// two never share a binary and fail at different points - Open here, Exec
// there - so do not assume one's behavior from the other.
type FailToOpenDriver struct{}

func (FailToOpenDriver) Open(_ context.Context, dsn string, _ int) (db.Pool, error) {
	return nil, fmt.Errorf("connection refused: %s", dsn)
}

// SlowDriver blocks Exec until its context is canceled — for cancel/timeout tests.
type SlowDriver struct{}

func (SlowDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
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
