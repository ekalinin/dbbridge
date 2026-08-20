package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	db.Register("postgres", fastDriver{})     // fast 2-row driver
	db.Register("mysql", streamDriver{})      // infinite-stream driver (cancel/timeout)
	db.Register("clickhouse", &identDriver{}) // pools with identity, for reload tests
	db.Register("oracle", bulkDriver{})       // many rows, for progress tests

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
	storage.Register(badCloseBackend, badCloseStore{})
	storage.Register(badDeleteBackend, badDeleteStore{})
	storage.Register(textOnlyBackend, textOnlyStore{})

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func newManager(t *testing.T) (*QueryManager, state.MetaStore) {
	t.Helper()
	return newManagerWith(t, "")
}

// newManagerWith builds a manager whose defaults section carries extra keys,
// for the settings that are fixed at construction time.
func newManagerWith(t *testing.T, extraDefaults string) (*QueryManager, state.MetaStore) {
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
  query_timeout: 30s
%s
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
  - id: bulkdb
    engine: oracle
    dsn: "oracle://fake"
    max_conns: 2
`, extraDefaults, resultsDir)

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

// newManagerWithStore builds a manager on a caller-supplied MetaStore, so a test
// can make persistence fail the way a Redis outage does.
func newManagerWithStore(t *testing.T, ms state.MetaStore) *QueryManager {
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
`, resultsDir)

	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(cfgContent), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgMgr, err := config.NewManager(path)
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}
	qm, err := NewQueryManager(cfgMgr, ms)
	if err != nil {
		t.Fatalf("NewQueryManager: %v", err)
	}
	t.Cleanup(func() { qm.Close() })
	return qm
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
		// Older than the reaper's grace period, which exists so a query that has
		// simply not been heartbeated yet is not failed while it runs fine.
		CreatedAt: time.Now().Add(-time.Minute),
		StartedAt: time.Now().Add(-time.Minute),
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

	qm.collectGarbage()

	if _, err := ms.GetQuery(context.Background(), "expired-1"); err == nil {
		t.Error("expected expired query metadata to be deleted")
	}
}

// TestWatch_UnsubscribeDuringNotify cancels subscriptions while notifications
// are in flight, which is what a client disconnecting during query completion
// does. It guards two races: sending into a channel that the unsubscribe path
// closes, and removing a watcher from the registry a notifier is iterating.
func TestWatch_UnsubscribeDuringNotify(t *testing.T) {
	qm, ms := newManager(t)

	const (
		queryID     = "watch-race"
		subscribers = 50
		notifiers   = 8
		events      = 200
	)

	// Watch resolves the record first, so the query has to exist.
	if err := ms.PutQuery(t.Context(), &domain.QueryRecord{
		ID: queryID, DatabaseID: "testdb", State: domain.StateRunning,
		OwnerInstanceID: "test-instance", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	var wg sync.WaitGroup
	for range subscribers {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := qm.Watch(ctx, queryID)
		if err != nil {
			cancel()
			t.Fatalf("Watch: %v", err)
		}
		wg.Go(func() {
			for ev := range ch {
				if ev.QueryID != queryID {
					t.Errorf("watch event for %q, want %q", ev.QueryID, queryID)
				}
			}
		})
		wg.Go(cancel)
	}

	for range notifiers {
		wg.Go(func() {
			for range events {
				qm.notifyWatchers(QueryEvent{QueryID: queryID, State: domain.StateRunning})
			}
		})
	}

	wg.Wait()
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

// ── regression tests for the audited defects ─────────────────────────────────

// TestSubmitQuery_RejectsTraversalFormat covers the path that let an
// unvalidated result_format create, truncate and delete an arbitrary file.
func TestSubmitQuery_RejectsTraversalFormat(t *testing.T) {
	qm, _ := newManager(t)
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("seed victim: %v", err)
	}

	rel, err := filepath.Rel(resultsDir, victim)
	if err != nil {
		t.Fatalf("rel: %v", err)
	}

	_, err = qm.SubmitQuery(context.Background(), "testdb", "SELECT 1",
		domain.QueryOptions{Mode: "async", ResultFormat: rel})
	if _, ok := errors.AsType[domain.ValidationError](err); !ok {
		t.Fatalf("SubmitQuery error = %v, want domain.ValidationError", err)
	}
	if data, err := os.ReadFile(victim); err != nil || string(data) != "keep me" {
		t.Fatalf("victim file was touched: data=%q err=%v", data, err)
	}
}

func TestSubmitQuery_RejectsUnknownOptions(t *testing.T) {
	qm, _ := newManager(t)
	cases := []struct {
		name string
		opts domain.QueryOptions
	}{
		{"format", domain.QueryOptions{Mode: "async", ResultFormat: "avro"}},
		{"mode", domain.QueryOptions{Mode: "SYNC"}},
		{"backend", domain.QueryOptions{Mode: "async", StorageBackend: "nope"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", tc.opts)
			if _, ok := errors.AsType[domain.ValidationError](err); !ok {
				t.Fatalf("SubmitQuery error = %v, want domain.ValidationError", err)
			}
		})
	}
}

func TestSubmitQuery_UnknownDatabaseIsNotFound(t *testing.T) {
	qm, _ := newManager(t)
	_, err := qm.SubmitQuery(context.Background(), "nosuchdb", "SELECT 1", domain.QueryOptions{})
	if _, ok := errors.AsType[domain.NotFoundError](err); !ok {
		t.Fatalf("SubmitQuery error = %v, want domain.NotFoundError", err)
	}
}

// TestSubmitQuery_RejectsWrites keeps DML and DDL out by default: an
// unauthenticated endpoint that runs them is remote code execution against the
// target database.
func TestSubmitQuery_RejectsWrites(t *testing.T) {
	qm, _ := newManager(t)
	for _, sql := range []string{"DELETE FROM t", "SELECT 1; DROP TABLE t", "UPDATE t SET a=1"} {
		_, err := qm.SubmitQuery(context.Background(), "testdb", sql, domain.QueryOptions{})
		if _, ok := errors.AsType[domain.ValidationError](err); !ok {
			t.Errorf("SubmitQuery(%q) error = %v, want domain.ValidationError", sql, err)
		}
	}
}

// TestSubmitQuery_StoresNormalizedOptions pins the defaults the spec declares
// but that were previously stored as empty values.
func TestSubmitQuery_StoresNormalizedOptions(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	final := pollTerminal(t, qm, rec.ID, 5*time.Second)
	if final.Options.Mode != "async" {
		t.Errorf("stored mode = %q, want async", final.Options.Mode)
	}
	if final.Options.ResultFormat != "jsonl" {
		t.Errorf("stored result_format = %q, want jsonl", final.Options.ResultFormat)
	}
	if final.Options.ResultTTL != time.Hour {
		t.Errorf("stored result_ttl = %v, want the configured default 1h", final.Options.ResultTTL)
	}
	// defaults.query_timeout is declared in the test config and was never applied.
	if final.Options.Timeout != 30*time.Second {
		t.Errorf("stored timeout = %v, want the configured default 30s", final.Options.Timeout)
	}
}

// TestRun_WriterCloseFailureFailsQuery covers I4: for S3 and ClickHouse it is
// Close that waits for the upload and returns its error, so ignoring it marked
// queries SUCCEEDED with no result behind them.
func TestRun_WriterCloseFailureFailsQuery(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1",
		domain.QueryOptions{Mode: "async", StorageBackend: badCloseBackend})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}

	final := pollTerminal(t, qm, rec.ID, 5*time.Second)
	if final.State != domain.StateFailed {
		t.Fatalf("state = %s, want FAILED", final.State)
	}
	if final.Error == nil || final.Error.Code != domain.ErrCodeStorageFinalizeFailed {
		t.Errorf("error = %+v, want code %s", final.Error, domain.ErrCodeStorageFinalizeFailed)
	}
	if final.Result != nil {
		t.Errorf("failed query carries a Result ref: %+v", final.Result)
	}
}

// TestRun_TerminalPersistFailureIsRetried covers I2 and I4: a failing terminal
// write used to be logged once and the query then dropped from the registry, so
// a Redis blip left the result materialized while the record stayed RUNNING and
// nothing ever came back to fix it.
func TestRun_TerminalPersistFailureIsRetried(t *testing.T) {
	inner := state.NewMemoryMetaStore()
	t.Cleanup(func() { inner.Close() })
	ms := &failingMetaStore{MetaStore: inner, failState: domain.StateSucceeded}

	qm := newManagerWithStore(t, ms)

	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}

	// The query stays owned while the write is being retried: that is what keeps
	// its lease alive and the owner reaper away from it.
	waitUntil(t, 5*time.Second, func() bool { return ms.attemptsFor(domain.StateSucceeded) >= 2 }, "a retry of the terminal write")
	if qm.activeForDB("testdb") == 0 {
		t.Error("the query left the registry while its terminal write was still being retried")
	}

	// And it is released once the retries are exhausted, rather than wedging.
	waitUntil(t, 15*time.Second, func() bool { return qm.activeForDB("testdb") == 0 }, "the query to be released")

	if got := ms.attemptsFor(domain.StateSucceeded); got != terminalPersistAttempts {
		t.Errorf("terminal write attempted %d times, want %d", got, terminalPersistAttempts)
	}

	// The record is left non-terminal on purpose: its lease stops being
	// refreshed, so the owner reaper picks it up instead of it being lost.
	stored, err := inner.GetQuery(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if stored.State.IsTerminal() {
		t.Errorf("state = %s, want a non-terminal state the reaper can act on", stored.State)
	}
}

// waitUntil polls cond until it holds or the deadline passes.
func waitUntil(t *testing.T, deadline time.Duration, cond func() bool, what string) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", deadline, what)
}

// TestReapStaleOwners_DoesNotOverwriteTerminal is the fencing regression: a
// reaper that writes unconditionally can replace a SUCCEEDED record, and its
// ResultRef, with FAILED/OWNER_LOST.
func TestReapStaleOwners_DoesNotOverwriteTerminal(t *testing.T) {
	qm, ms := newManager(t)
	rec := &domain.QueryRecord{
		ID:              "done-1",
		DatabaseID:      "testdb",
		State:           domain.StateSucceeded,
		OwnerInstanceID: "dead-instance",
		CreatedAt:       time.Now().Add(-time.Minute),
		FinishedAt:      time.Now(),
		Result:          &domain.ResultRef{Backend: "fs", Locator: "x", Format: "jsonl"},
	}
	if err := ms.PutQuery(t.Context(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	qm.reapStaleOwners()

	got, err := ms.GetQuery(t.Context(), "done-1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StateSucceeded {
		t.Errorf("state = %s, want SUCCEEDED", got.State)
	}
	if got.Result == nil {
		t.Error("the reaper dropped the ResultRef")
	}
}

// TestReapStaleOwners_SkipsOwnQueries: a query owned by this instance is not
// somebody else's to fail, even when it is not in the local registry.
func TestReapStaleOwners_SkipsOwnQueries(t *testing.T) {
	qm, ms := newManager(t)
	rec := &domain.QueryRecord{
		ID:              "mine-1",
		DatabaseID:      "testdb",
		State:           domain.StateRunning,
		OwnerInstanceID: "test-instance",
		CreatedAt:       time.Now().Add(-time.Minute),
	}
	if err := ms.PutQuery(t.Context(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	qm.reapStaleOwners()

	got, err := ms.GetQuery(t.Context(), "mine-1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StateRunning {
		t.Errorf("state = %s, want RUNNING", got.State)
	}
}

// TestReapStaleOwners_GracePeriod: a query created moments ago has not been
// heartbeated yet and must not be mistaken for an orphan.
func TestReapStaleOwners_GracePeriod(t *testing.T) {
	qm, ms := newManager(t)
	rec := &domain.QueryRecord{
		ID:              "fresh-1",
		DatabaseID:      "testdb",
		State:           domain.StatePending,
		OwnerInstanceID: "other-instance",
		CreatedAt:       time.Now(),
	}
	if err := ms.PutQuery(t.Context(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	qm.reapStaleOwners()

	got, err := ms.GetQuery(t.Context(), "fresh-1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StatePending {
		t.Errorf("state = %s, want PENDING", got.State)
	}
}

// TestCollectGarbage_KeepsMetadataWhenStorageDeleteFails: dropping the record
// while the object is still there strips the only reference to it, so a retry
// becomes impossible.
func TestCollectGarbage_KeepsMetadataWhenStorageDeleteFails(t *testing.T) {
	qm, ms := newManager(t)
	rec := &domain.QueryRecord{
		ID:              "expired-2",
		DatabaseID:      "testdb",
		State:           domain.StateSucceeded,
		OwnerInstanceID: "test-instance",
		CreatedAt:       time.Now().Add(-2 * time.Hour),
		FinishedAt:      time.Now().Add(-1 * time.Hour),
		Options:         domain.QueryOptions{ResultTTL: time.Minute},
		Result:          &domain.ResultRef{Backend: badDeleteBackend, Locator: "x", Format: "jsonl"},
	}
	if err := ms.PutQuery(t.Context(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	qm.collectGarbage()

	got, err := ms.GetQuery(t.Context(), "expired-2")
	if err != nil {
		t.Fatalf("metadata was deleted despite a storage cleanup failure: %v", err)
	}
	if got.State != domain.StateExpired {
		t.Errorf("state = %s, want EXPIRED", got.State)
	}
}

// TestDrain_ClosesAdmission covers the I5 race: the draining check used to be a
// separate step from registration, so a query could still register after the
// shutdown loop had observed zero in-flight queries.
func TestDrain_ClosesAdmission(t *testing.T) {
	qm, _ := newManager(t)
	qm.Drain()

	_, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{})
	if _, ok := errors.AsType[domain.DrainingError](err); !ok {
		t.Fatalf("SubmitQuery error = %v, want domain.DrainingError", err)
	}
	if n := qm.CountInFlight(context.Background()); n != 0 {
		t.Errorf("CountInFlight = %d after a rejected submission, want 0", n)
	}
}

// TestWatch_UnknownQueryIsNotFound: a watch that never resolves is worse than
// an error, the subscriber waits for an event that cannot arrive.
func TestWatch_UnknownQueryIsNotFound(t *testing.T) {
	qm, _ := newManager(t)
	if _, err := qm.Watch(t.Context(), "no-such-query"); err == nil {
		t.Fatal("Watch accepted an unknown query id")
	}
}

// TestWatch_TerminalQueryClosesImmediately: subscribing after the query has
// finished must yield its final state and end, not block.
func TestWatch_TerminalQueryClosesImmediately(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{Mode: "sync"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}

	ch, err := qm.Watch(t.Context(), rec.ID)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	var got []QueryEvent
	for ev := range ch {
		got = append(got, ev)
	}
	if len(got) != 1 || got[0].State != domain.StateSucceeded {
		t.Fatalf("events = %+v, want a single SUCCEEDED event", got)
	}
}

func TestRecoverOrphans_FailsOwnRunningQueries(t *testing.T) {
	qm, ms := newManager(t)
	rec := &domain.QueryRecord{
		ID:              "leftover-1",
		DatabaseID:      "testdb",
		State:           domain.StateRunning,
		OwnerInstanceID: "test-instance",
		CreatedAt:       time.Now().Add(-time.Hour),
	}
	if err := ms.PutQuery(t.Context(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	qm.recoverOrphans()

	got, err := ms.GetQuery(t.Context(), "leftover-1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StateFailed {
		t.Fatalf("state = %s, want FAILED", got.State)
	}
	if got.Error == nil || got.Error.Code != domain.ErrCodeOwnerLost {
		t.Errorf("error = %+v, want code OWNER_LOST", got.Error)
	}
}

// ── failing backends ─────────────────────────────────────────────────────────

// failingMetaStore rejects writes of one particular state, which is what a
// MetaStore outage looks like at exactly the wrong moment.
type failingMetaStore struct {
	state.MetaStore
	failState domain.QueryState

	mu       sync.Mutex
	attempts map[domain.QueryState]int
}

func (f *failingMetaStore) PutQuery(ctx context.Context, rec *domain.QueryRecord) error {
	if rec.State != f.failState {
		return f.MetaStore.PutQuery(ctx, rec)
	}
	f.mu.Lock()
	if f.attempts == nil {
		f.attempts = make(map[domain.QueryState]int)
	}
	f.attempts[rec.State]++
	f.mu.Unlock()
	return errors.New("metastore unavailable")
}

func (f *failingMetaStore) attemptsFor(s domain.QueryState) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[s]
}

const (
	badCloseBackend  = "badclose"
	badDeleteBackend = "baddelete"
	textOnlyBackend  = "textonly"
)

// badCloseStore accepts every write and fails in Close, the way an S3 upload
// that only fails when the multipart request completes does.
type badCloseStore struct{}

func (badCloseStore) Writer(_ context.Context, _ string, format string) (io.WriteCloser, domain.ResultRef, error) {
	return failingCloser{}, domain.ResultRef{Backend: badCloseBackend, Locator: "x", Format: format}, nil
}
func (badCloseStore) Reader(_ context.Context, _ domain.ResultRef) (io.ReadCloser, error) {
	return nil, errors.New("not supported")
}
func (badCloseStore) Stat(_ context.Context, ref domain.ResultRef) (domain.ResultRef, error) {
	return ref, nil
}
func (badCloseStore) Delete(_ context.Context, _ domain.ResultRef) error { return nil }

type failingCloser struct{}

func (failingCloser) Write(p []byte) (int, error) { return len(p), nil }
func (failingCloser) Close() error                { return errors.New("upload did not complete") }

// textOnlyStore stands in for the ClickHouse backend: it stores the result as
// rows of text, so a binary format does not survive the round trip.
type textOnlyStore struct{ badCloseStore }

func (textOnlyStore) SupportsFormat(format string) bool { return format != "parquet" }

// badDeleteStore fails cleanup, which is what a storage outage looks like to GC.
type badDeleteStore struct{ badCloseStore }

func (badDeleteStore) Delete(_ context.Context, _ domain.ResultRef) error {
	return errors.New("storage unavailable")
}

// TestSubmitQuery_ConcurrencyLimit: without a cap, any client can open as many
// queries as it likes, each holding a pool connection and a result file.
func TestSubmitQuery_ConcurrencyLimit(t *testing.T) {
	qm, _ := newManagerWith(t, "  max_concurrent_queries: 1")

	first, err := qm.SubmitQuery(context.Background(), "streamdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("first SubmitQuery: %v", err)
	}
	pollState(t, qm, first.ID, domain.StateRunning, 3*time.Second)

	_, err = qm.SubmitQuery(context.Background(), "streamdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
	if _, ok := errors.AsType[domain.ResourceExhaustedError](err); !ok {
		t.Fatalf("second SubmitQuery error = %v, want domain.ResourceExhaustedError", err)
	}

	if err := qm.StopQuery(context.Background(), first.ID); err != nil {
		t.Fatalf("StopQuery: %v", err)
	}
	pollTerminal(t, qm, first.ID, 3*time.Second)

	// The slot is released when the query goroutine returns.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err = qm.SubmitQuery(context.Background(), "streamdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot was never released: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSubmitQuery_AllowWrites keeps the guard opt-out honest: operators that
// need DML must be able to turn it off explicitly.
func TestSubmitQuery_AllowWrites(t *testing.T) {
	qm, _ := newManagerWith(t, "  allow_writes: true")
	if _, err := qm.SubmitQuery(context.Background(), "testdb", "DELETE FROM t", domain.QueryOptions{}); err != nil {
		t.Fatalf("SubmitQuery with allow_writes: %v", err)
	}
}

// TestReload_RecreatesChangedPools: syncPools used to reuse a pool by ID alone,
// so a changed DSN, engine or max_conns was reported as "updated" and never
// applied.
func TestReload_RecreatesChangedPools(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "cfg.yaml")
	write := func(maxConns int) {
		body := fmt.Sprintf(`
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
    engine: clickhouse
    dsn: "clickhouse://fake"
    max_conns: %d
`, resultsDir, maxConns)
		if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}

	write(2)
	cfgMgr, err := config.NewManager(cfgPath)
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

	before, _ := qm.GetPool("testdb")

	write(8)
	report, err := qm.Reload()
	if err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if len(report.Updated) != 1 || report.Updated[0] != "testdb" {
		t.Fatalf("report.Updated = %v, want [testdb]", report.Updated)
	}

	after, _ := qm.GetPool("testdb")
	if before == after {
		t.Error("the pool was reused although max_conns changed")
	}
}

// identDriver hands out pools that are distinguishable from one another, which
// is what a reload test needs: the other fakes are empty structs and compare
// equal even when the pool really was recreated.
type identDriver struct{ n atomic.Int64 }

func (d *identDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return &identPool{id: d.n.Add(1)}, nil
}

type identPool struct{ id int64 }

func (p *identPool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return &fastStream{rows: [][]any{{p.id}}, pos: -1, cols: []string{"id"}}, nil
}
func (p *identPool) Ping(_ context.Context) error { return nil }
func (p *identPool) Stat() db.PoolStat            { return db.PoolStat{} }
func (p *identPool) Close() error                 { return nil }

// TestRun_ComputesChecksum: ResultRef.Checksum was always empty, so a download
// could not be verified against what was written.
func TestRun_ComputesChecksum(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1",
		domain.QueryOptions{Mode: "sync", ResultFormat: "jsonl"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	if rec.Result == nil {
		t.Fatal("no result ref")
	}
	if !strings.HasPrefix(rec.Result.Checksum, "sha256:") {
		t.Fatalf("checksum = %q, want a sha256: prefix", rec.Result.Checksum)
	}

	store, err := storage.GetStore(rec.Result.Backend)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	reader, err := store.Reader(context.Background(), *rec.Result)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer reader.Close()

	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		t.Fatalf("read result: %v", err)
	}
	want := "sha256:" + hex.EncodeToString(sum.Sum(nil))
	if rec.Result.Checksum != want {
		t.Errorf("checksum = %q, want %q for the stored bytes", rec.Result.Checksum, want)
	}
}

// TestSubmitQuery_ParquetProducesParquet: the format used to serialize JSONL
// into a .parquet file.
func TestSubmitQuery_ParquetProducesParquet(t *testing.T) {
	qm, _ := newManager(t)
	rec, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1",
		domain.QueryOptions{Mode: "sync", ResultFormat: "parquet"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	if rec.State != domain.StateSucceeded {
		t.Fatalf("state = %s, want SUCCEEDED", rec.State)
	}

	store, _ := storage.GetStore(rec.Result.Backend)
	reader, err := store.Reader(context.Background(), *rec.Result)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer reader.Close()

	magic := make([]byte, 4)
	if _, err := io.ReadFull(reader, magic); err != nil {
		t.Fatalf("read magic: %v", err)
	}
	if string(magic) != "PAR1" {
		t.Errorf("result starts with %q, want the parquet magic PAR1", magic)
	}
}

// TestSubmitQuery_RejectsFormatTheBackendCannotHold: the pair
// result_format: parquet + a line-oriented backend became reachable once both
// halves were enabled, and it produced a file whose "PAR1" footer came back as
// "AR1\n" - unreadable, and with a checksum that no longer matched.
func TestSubmitQuery_RejectsFormatTheBackendCannotHold(t *testing.T) {
	qm, _ := newManager(t)

	_, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{
		Mode:           "sync",
		ResultFormat:   "parquet",
		StorageBackend: textOnlyBackend,
	})
	if _, ok := errors.AsType[domain.ValidationError](err); !ok {
		t.Fatalf("err = %v, want a ValidationError", err)
	}

	// The text formats the backend does hold are still accepted.
	if _, err := qm.SubmitQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{
		Mode:           "sync",
		ResultFormat:   "jsonl",
		StorageBackend: textOnlyBackend,
	}); err != nil {
		t.Errorf("jsonl on the same backend was rejected: %v", err)
	}
}

// TestWatch_AcrossInstances covers I2: watchers live in a per-process map, so
// before events were published through the MetaStore a subscription opened on
// any instance other than the owner never received anything.
func TestWatch_AcrossInstances(t *testing.T) {
	ms := state.NewMemoryMetaStore()
	t.Cleanup(func() { ms.Close() })

	owner := newManagerOn(t, ms, "instance-a")
	reader := newManagerOn(t, ms, "instance-b")

	rec, err := owner.SubmitQuery(context.Background(), "streamdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}

	// Subscribe through the instance that does not own the query.
	ch, err := reader.Watch(t.Context(), rec.ID)
	if err != nil {
		t.Fatalf("Watch on the non-owner instance: %v", err)
	}

	if err := owner.StopQuery(context.Background(), rec.ID); err != nil {
		t.Fatalf("StopQuery: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("the subscription closed without a terminal event")
			}
			if ev.State.IsTerminal() {
				if ev.State != domain.StateCanceled {
					t.Errorf("terminal state = %s, want CANCELED", ev.State)
				}
				return
			}
		case <-deadline:
			t.Fatal("no terminal event reached the non-owner instance")
		}
	}
}

// TestRun_PublishesProgress covers stats that used to be written only once, at
// completion, so a long query reported nothing until it was already over.
func TestRun_PublishesProgress(t *testing.T) {
	qm, _ := newManager(t)

	rec, err := qm.SubmitQuery(context.Background(), "bulkdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}

	ch, err := qm.Watch(t.Context(), rec.ID)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("no progress event before the query finished")
			}
			if ev.State == domain.StateRunning && ev.Stats.RowsRead > 0 {
				if ev.Stats.BytesWritten == 0 {
					t.Errorf("progress reported %d rows but 0 bytes", ev.Stats.RowsRead)
				}
				return
			}
			if ev.State.IsTerminal() {
				t.Fatal("the query finished without ever reporting progress")
			}
		case <-deadline:
			t.Fatal("no progress event")
		}
	}
}

// newManagerOn builds a manager with a given instance ID on a shared MetaStore,
// which is what a multi-node deployment looks like.
func newManagerOn(t *testing.T, ms state.MetaStore, instanceID string) *QueryManager {
	t.Helper()
	cfgContent := fmt.Sprintf(`
instance:
  id: %s
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
  - id: streamdb
    engine: mysql
    dsn: "mysql://fake"
    max_conns: 2
`, instanceID, resultsDir)

	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(cfgContent), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgMgr, err := config.NewManager(path)
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}
	qm, err := NewQueryManager(cfgMgr, ms)
	if err != nil {
		t.Fatalf("NewQueryManager: %v", err)
	}
	t.Cleanup(func() { qm.Close() })
	return qm
}

// bulkDriver emits many rows quickly, so streaming progress is observable
// without waiting on wall-clock time.
type bulkDriver struct{}

func (bulkDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) { return bulkPool{}, nil }

type bulkPool struct{}

func (bulkPool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return &bulkStream{total: 5000}, nil
}
func (bulkPool) Ping(_ context.Context) error { return nil }
func (bulkPool) Stat() db.PoolStat            { return db.PoolStat{} }
func (bulkPool) Close() error                 { return nil }

type bulkStream struct {
	total int
	n     int
}

func (s *bulkStream) Columns() ([]string, error) { return []string{"id"}, nil }
func (s *bulkStream) Next() bool {
	if s.n >= s.total {
		return false
	}
	s.n++
	// Slow enough that the terminal state cannot beat the progress event.
	time.Sleep(200 * time.Microsecond)
	return true
}
func (s *bulkStream) Scan(dest ...any) error {
	if p, ok := dest[0].(*any); ok {
		*p = int64(s.n)
	}
	return nil
}
func (s *bulkStream) Err() error   { return nil }
func (s *bulkStream) Close() error { return nil }

// TestRun_ProgressDoesNotResurrectAFinalizedRecord: progress used to be written
// with an unconditional PutQuery, so a record another instance's reaper had
// already failed went back to RUNNING with its error and FinishedAt cleared,
// and rejoined this instance's in-flight set with no lease behind it - so the
// reaper failed it again on the next tick, announcing a false terminal state to
// the cluster every couple of seconds.
func TestRun_ProgressDoesNotResurrectAFinalizedRecord(t *testing.T) {
	qm, ms := newManager(t)
	ctx := context.Background()

	rec, err := qm.SubmitQuery(ctx, "streamdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	pollState(t, qm, rec.ID, domain.StateRunning, 3*time.Second)

	// Wait for the reporter to write at least once, so the test is not just
	// racing a reporter that never ran.
	waitUntil(t, 6*time.Second, func() bool {
		cur, err := ms.GetQuery(ctx, rec.ID)
		return err == nil && cur.Stats.RowsRead > 0
	}, "the first progress report")

	// Another instance's reaper fences the record.
	stored, err := ms.GetQuery(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	stored.State = domain.StateFailed
	stored.Error = domain.NewQueryError(domain.ErrCodeOwnerLost, "owner lost", true)
	stored.FinishedAt = time.Now()
	written, err := ms.UpdateQueryIfState(ctx, stored, domain.StatePending, domain.StateRunning)
	if err != nil || !written {
		t.Fatalf("fencing write: written=%v err=%v", written, err)
	}

	// Several report intervals have to pass without the verdict being undone.
	deadline := time.Now().Add(2*progressPersistInterval + time.Second)
	for time.Now().Before(deadline) {
		cur, err := ms.GetQuery(ctx, rec.ID)
		if err != nil {
			t.Fatalf("GetQuery: %v", err)
		}
		if cur.State != domain.StateFailed {
			t.Fatalf("progress put the record back to %s after it had been failed", cur.State)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestControl_RemoteEventDoesNotCloseTheOwnersWatchers: a terminal event
// published by another instance was delivered to local subscriptions without
// checking whether this instance is the one executing the query. It closed the
// watch stream and made a sync submission return FAILED for a query that was
// still running and went on to succeed.
func TestControl_RemoteEventDoesNotCloseTheOwnersWatchers(t *testing.T) {
	ms := state.NewMemoryMetaStore()
	t.Cleanup(func() {
		if err := ms.Close(); err != nil {
			t.Errorf("close metastore: %v", err)
		}
	})

	owner := newManagerOn(t, ms, "owner")
	ctx := context.Background()

	rec, err := owner.SubmitQuery(ctx, "streamdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("SubmitQuery: %v", err)
	}
	pollState(t, owner, rec.ID, domain.StateRunning, 3*time.Second)

	ch, err := owner.Watch(t.Context(), rec.ID)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Drain the immediate snapshot the subscription starts with.
	<-ch

	if err := ms.PublishControl(ctx, state.ControlMsg{
		Type:     state.ControlQueryEvent,
		QueryID:  rec.ID,
		SenderID: "some-other-instance",
		Event: &state.QueryEventPayload{
			State: string(domain.StateFailed),
			Error: domain.NewQueryError(domain.ErrCodeOwnerLost, "owner lost", true),
		},
	}); err != nil {
		t.Fatalf("PublishControl: %v", err)
	}

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("the subscription was closed by another instance's verdict on a query this one owns")
		}
		if ev.State.IsTerminal() {
			t.Fatalf("a remote %s reached a watcher of a query this instance is still running", ev.State)
		}
	case <-time.After(time.Second):
	}

	// And the query really is still running.
	cur, err := owner.GetQuery(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if cur.State != domain.StateRunning {
		t.Errorf("state = %s, want RUNNING", cur.State)
	}
}
