package manager

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/storage/backends/fs"
)

var resultsDir string

func TestMain(m *testing.M) {
	db.Register("postgres", fastDriver{}) // fast 2-row driver
	db.Register("mysql", streamDriver{})  // infinite-stream driver (cancel/timeout)

	dir, err := os.MkdirTemp("", "dbbridge-mgr-test-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	resultsDir = dir

	fsStore, err := fs.NewFSResultStore(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	storage.Register("fs", fsStore)

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func newManager(t *testing.T) (*QueryManager, state.MetaStore) {
	t.Helper()
	cfgContent := fmt.Sprintf(`
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
    dsn: "postgres://fake"
    max_conns: 2
  - id: streamdb
    engine: mysql
    dsn: "mysql://fake"
    max_conns: 2
`, resultsDir)

	cfgFile, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("temp config: %v", err)
	}
	if _, err := cfgFile.WriteString(cfgContent); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgFile.Close()

	cfgMgr, err := config.NewManager(cfgFile.Name())
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}
	ms := state.NewMemoryMetaStore()
	t.Cleanup(func() { ms.Close() })

	qm, err := NewQueryManager(cfgMgr, ms)
	if err != nil {
		t.Fatalf("NewQueryManager: %v", err)
	}
	t.Cleanup(func() { qm.Close() })
	return qm, ms
}

func TestReloadReport(t *testing.T) {
	qm, _ := newManager(t)

	report, err := qm.Reload()
	if err != nil {
		t.Fatalf("Reload: %v", err)
	}
	// The config file is unchanged between initial load and reload, so the diff
	// must be empty across all three buckets.
	if len(report.Added) != 0 || len(report.Removed) != 0 || len(report.Updated) != 0 {
		t.Errorf("expected empty reload report on unchanged config, got %+v", report)
	}
}

func pollState(t *testing.T, qm *QueryManager, id string, want domain.QueryState, deadline time.Duration) *domain.QueryRecord {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		rec, err := qm.GetQuery(context.Background(), id)
		if err == nil && rec.State == want {
			return rec
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec, _ := qm.GetQuery(context.Background(), id)
	got := domain.QueryState("<nil>")
	if rec != nil {
		got = rec.State
	}
	t.Fatalf("query %s did not reach %s within %v (last state %s)", id, want, deadline, got)
	return nil
}

func pollTerminal(t *testing.T, qm *QueryManager, id string, deadline time.Duration) *domain.QueryRecord {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		rec, err := qm.GetQuery(context.Background(), id)
		if err == nil && rec.State.IsTerminal() {
			return rec
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("query %s did not reach a terminal state within %v", id, deadline)
	return nil
}

func TestSubmitQuery_Async(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	if rec.ID == "" {
		t.Fatal("empty query id")
	}
	final := pollState(t, qm, rec.ID, domain.StateSucceeded, 5*time.Second)
	if final.Stats.RowsRead != 2 {
		t.Errorf("rows_read = %d, want 2", final.Stats.RowsRead)
	}
	if final.Result == nil {
		t.Error("expected a Result ref")
	}
}

// TestSubmitQuery_SyncNoHang is the regression test for the watcher race: a fast
// sync query must always return a terminal record and never hang.
func TestSubmitQuery_SyncNoHang(t *testing.T) {
	qm, _ := newManager(t)
	for i := range 25 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rec, err := qm.SubmitQuery(ctx, "testdb", "SELECT 1", domain.QueryOptions{Mode: "sync"})
		cancel()
		if err != nil {
			t.Fatalf("iter %d SubmitQuery: %v", i, err)
		}
		if rec.State != domain.StateSucceeded {
			t.Fatalf("iter %d: sync query state = %s, want SUCCEEDED", i, rec.State)
		}
	}
}

func TestSubmitQuery_Idempotency(t *testing.T) {
	qm, _ := newManager(t)
	opts := domain.QueryOptions{Mode: "sync", IdempotencyKey: "key-1"}
	rec1, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", opts)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	rec2, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", opts)
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if rec1.ID != rec2.ID {
		t.Errorf("idempotency: ids differ %s vs %s", rec1.ID, rec2.ID)
	}
}

func TestStopQuery_CancelsRunning(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "streamdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	pollState(t, qm, rec.ID, domain.StateRunning, 3*time.Second)

	if err := qm.StopQuery(context.Background(), rec.ID); err != nil {
		t.Fatalf("StopQuery: %v", err)
	}
	final := pollTerminal(t, qm, rec.ID, 3*time.Second)
	if final.State != domain.StateCanceled {
		t.Errorf("state after stop = %s, want CANCELED", final.State)
	}
}

func TestSubmitQuery_Timeout(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "streamdb", "SELECT *",
		domain.QueryOptions{Mode: "async", Timeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	final := pollTerminal(t, qm, rec.ID, 3*time.Second)
	// A timed-out stream surfaces as DeadlineExceeded → FAILED (not SUCCEEDED).
	if final.State == domain.StateSucceeded {
		t.Errorf("timed-out query unexpectedly SUCCEEDED")
	}
}

func TestReapStaleOwners_OwnerLost(t *testing.T) {
	qm, ms := newManager(t)
	// A RUNNING query owned by an instance that never heartbeats.
	rec := &domain.QueryRecord{
		ID:              "orphan-1",
		DatabaseID:      "testdb",
		SQL:             "SELECT 1",
		State:           domain.StateRunning,
		OwnerInstanceID: "dead-instance",
		CreatedAt:       time.Now(),
		StartedAt:       time.Now(),
	}
	if err := ms.PutQuery(context.Background(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	qm.reapStaleOwners()

	got, err := ms.GetQuery(context.Background(), "orphan-1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StateFailed {
		t.Fatalf("state = %s, want FAILED", got.State)
	}
	if got.Error == nil || got.Error.Code != "OWNER_LOST" {
		t.Errorf("error = %+v, want code OWNER_LOST", got.Error)
	}
}

func TestCollectGarbage_ExpiresAndDeletes(t *testing.T) {
	qm, ms := newManager(t)
	// A terminal query whose ResultTTL already elapsed.
	rec := &domain.QueryRecord{
		ID:              "expired-1",
		DatabaseID:      "testdb",
		State:           domain.StateSucceeded,
		OwnerInstanceID: "test-instance",
		CreatedAt:       time.Now().Add(-2 * time.Hour),
		FinishedAt:      time.Now().Add(-1 * time.Hour),
		Options:         domain.QueryOptions{ResultTTL: time.Minute},
	}
	if err := ms.PutQuery(context.Background(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	// Subscribe to observe the EXPIRED transition before deletion.
	ch, _ := qm.Watch(t.Context(), "expired-1")

	qm.collectGarbage()

	select {
	case ev := <-ch:
		if ev.State != domain.StateExpired {
			t.Errorf("watch event state = %s, want EXPIRED", ev.State)
		}
	case <-time.After(2 * time.Second):
		t.Error("did not receive EXPIRED watch event")
	}

	if _, err := ms.GetQuery(context.Background(), "expired-1"); err == nil {
		t.Error("expected expired query metadata to be deleted")
	}
}

// ── fakes ────────────────────────────────────────────────────────────────────

type fastDriver struct{}

func (fastDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) { return fastPool{}, nil }

type fastPool struct{}

func (fastPool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return &fastStream{rows: [][]any{{int64(1), "alice"}, {int64(2), "bob"}}, pos: -1, cols: []string{"id", "name"}}, nil
}
func (fastPool) Ping(_ context.Context) error { return nil }
func (fastPool) Stat() db.PoolStat            { return db.PoolStat{} }
func (fastPool) Close() error                 { return nil }

type fastStream struct {
	cols []string
	rows [][]any
	pos  int
}

func (s *fastStream) Columns() ([]string, error) { return s.cols, nil }
func (s *fastStream) Next() bool                 { s.pos++; return s.pos < len(s.rows) }
func (s *fastStream) Scan(dest ...any) error {
	for i, d := range dest {
		if p, ok := d.(*any); ok {
			*p = s.rows[s.pos][i]
		}
	}
	return nil
}
func (s *fastStream) Err() error   { return nil }
func (s *fastStream) Close() error { return nil }

// streamDriver returns an unbounded stream so the query stays RUNNING until canceled.
type streamDriver struct{}

func (streamDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return streamPool{}, nil
}

type streamPool struct{}

func (streamPool) Exec(_ context.Context, _ string) (db.RowStream, error) { return &infStream{}, nil }
func (streamPool) Ping(_ context.Context) error                           { return nil }
func (streamPool) Stat() db.PoolStat                                      { return db.PoolStat{} }
func (streamPool) Close() error                                           { return nil }

type infStream struct{ n int }

func (s *infStream) Columns() ([]string, error) { return []string{"id"}, nil }
func (s *infStream) Next() bool                 { time.Sleep(2 * time.Millisecond); s.n++; return true }
func (s *infStream) Scan(dest ...any) error {
	if p, ok := dest[0].(*any); ok {
		*p = int64(s.n)
	}
	return nil
}
func (s *infStream) Err() error   { return nil }
func (s *infStream) Close() error { return nil }
