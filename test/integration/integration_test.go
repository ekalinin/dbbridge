//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/storage/backends/s3"
)

func pollTerminal(t *testing.T, h *harness, id string, deadline time.Duration) *domain.QueryRecord {
	t.Helper()
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		rec, err := h.svc.GetQueryStatus(context.Background(), id)
		if err == nil && rec.State.IsTerminal() {
			return rec
		}
		// Without this a record that was deleted, or a MetaStore that stopped
		// answering, reads as a plain timeout and hides the real cause.
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("query %s did not reach a terminal state within %v (last error: %v)", id, deadline, lastErr)
	return nil
}

func readResult(t *testing.T, h *harness, id string) []map[string]any {
	t.Helper()
	reader, _, err := h.svc.DownloadResult(context.Background(), id, 0, 0)
	if err != nil {
		t.Fatalf("DownloadResult: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()

	var rows []map[string]any
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("parse JSONL %q: %v", line, err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan result: %v", err)
	}
	return rows
}

// TestPostgres_EndToEnd runs a query against a real PostgreSQL through a real
// Redis MetaStore and reads the materialized result back.
func TestPostgres_EndToEnd(t *testing.T) {
	redisAddr := startRedis(t)
	dsn := startPostgres(t)

	h := newHarness(t, harnessOptions{
		instanceID: "it-pg",
		redisAddr:  redisAddr,
		databases:  pgDatabases(dsn),
	})

	rec, err := h.svc.StartQuery(context.Background(), "pg", "SELECT id, name FROM users ORDER BY id",
		domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	final := pollTerminal(t, h, rec.ID, 30*time.Second)
	if final.State != domain.StateSucceeded {
		t.Fatalf("state = %s, error = %+v", final.State, final.Error)
	}
	if final.Stats.RowsRead != 2 {
		t.Errorf("rows_read = %d, want 2", final.Stats.RowsRead)
	}
	if final.Result == nil || !strings.HasPrefix(final.Result.Checksum, "sha256:") {
		t.Errorf("result ref = %+v, want a sha256 checksum", final.Result)
	}

	rows := readResult(t, h, rec.ID)
	if len(rows) != 2 || rows[0]["name"] != "alice" || rows[1]["name"] != "bob" {
		t.Errorf("rows = %+v, want alice and bob", rows)
	}
}

// TestMySQL_EndToEnd is the same round trip through the MySQL driver, whose
// type mapping differs (values arrive as []byte).
func TestMySQL_EndToEnd(t *testing.T) {
	redisAddr := startRedis(t)
	dsn := startMySQL(t)

	h := newHarness(t, harnessOptions{
		instanceID: "it-my",
		redisAddr:  redisAddr,
		databases:  mysqlDatabases(dsn),
	})

	rec, err := h.svc.StartQuery(context.Background(), "my", "SELECT id, name FROM users ORDER BY id",
		domain.QueryOptions{Mode: "sync"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	if rec.State != domain.StateSucceeded {
		t.Fatalf("state = %s, error = %+v", rec.State, rec.Error)
	}

	rows := readResult(t, h, rec.ID)
	if len(rows) != 2 || rows[0]["name"] != "alice" {
		t.Errorf("rows = %+v, want alice first", rows)
	}
}

// TestS3_ResultRoundTrip materializes into MinIO, which is the only path that
// exercises the Close-waits-for-upload contract behind I4, and - through the
// large result below - the multipart uploader.
func TestS3_ResultRoundTrip(t *testing.T) {
	redisAddr := startRedis(t)
	dsn := startPostgres(t)
	minio := startMinIO(t)

	store, err := s3.NewS3ResultStore(context.Background(), minio.bucket, "us-east-1",
		minio.endpoint, minio.keyID, minio.secret)
	if err != nil {
		t.Fatalf("NewS3ResultStore: %v", err)
	}
	if _, err := storage.GetStore("s3"); err != nil {
		storage.Register("s3", store)
	}

	h := newHarness(t, harnessOptions{
		instanceID:     "it-s3",
		redisAddr:      redisAddr,
		databases:      pgDatabases(dsn),
		defaultStorage: "s3",
		storageSection: fmt.Sprintf("  fs:\n    root: %s\n  s3:\n    bucket: %s\n", resultsRoot, minio.bucket),
	})

	rec, err := h.svc.StartQuery(context.Background(), "pg", "SELECT id, name FROM users ORDER BY id",
		domain.QueryOptions{Mode: "sync", StorageBackend: "s3"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	if rec.State != domain.StateSucceeded {
		t.Fatalf("state = %s, error = %+v", rec.State, rec.Error)
	}
	if rec.Result == nil || rec.Result.Backend != "s3" {
		t.Fatalf("result ref = %+v, want the s3 backend", rec.Result)
	}

	rows := readResult(t, h, rec.ID)
	if len(rows) != 2 {
		t.Errorf("rows = %+v, want 2", rows)
	}

	// Stat has to see the object the upload actually produced.
	stat, err := store.Stat(context.Background(), *rec.Result)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.SizeBytes != rec.Result.SizeBytes {
		t.Errorf("stat size = %d, recorded %d", stat.SizeBytes, rec.Result.SizeBytes)
	}

	if err := store.Delete(context.Background(), *rec.Result); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Stat(context.Background(), *rec.Result); err == nil {
		t.Error("Stat succeeded after Delete")
	}

	// The uploader's part size is 5 MiB, and for a non-seekable body it falls
	// back to a plain PutObject when the first part reaches EOF. The two-row
	// result above never left that fast path, so breaking part assembly or
	// completion would not have shown up here. This one does not fit.
	const (
		wantRows = 40000
		padding  = 200
	)
	big, err := h.svc.StartQuery(context.Background(), "pg",
		fmt.Sprintf("SELECT g AS id, repeat('x', %d) AS pad FROM generate_series(1, %d) g", padding, wantRows),
		domain.QueryOptions{Mode: "sync", StorageBackend: "s3"})
	if err != nil {
		t.Fatalf("StartQuery (multipart): %v", err)
	}
	if big.State != domain.StateSucceeded {
		t.Fatalf("state = %s, error = %+v", big.State, big.Error)
	}
	if big.Result == nil {
		t.Fatal("no result ref for the large query")
	}
	if big.Result.SizeBytes <= multipartPartSize {
		t.Fatalf("result is %d bytes, want more than the %d-byte part size", big.Result.SizeBytes, multipartPartSize)
	}
	if big.Result.RowCount != wantRows {
		t.Errorf("row count = %d, want %d", big.Result.RowCount, wantRows)
	}

	// Every part has to have been assembled in order and completed: the object
	// the uploader produced must be exactly the one that was written.
	bigStat, err := store.Stat(context.Background(), *big.Result)
	if err != nil {
		t.Fatalf("Stat (multipart): %v", err)
	}
	if bigStat.SizeBytes != big.Result.SizeBytes {
		t.Errorf("stat size = %d, recorded %d", bigStat.SizeBytes, big.Result.SizeBytes)
	}
	if got := countResultRows(t, h, big.ID); got != wantRows {
		t.Errorf("read back %d rows, want %d", got, wantRows)
	}
	if err := store.Delete(context.Background(), *big.Result); err != nil {
		t.Errorf("Delete (multipart): %v", err)
	}
}

// multipartPartSize is the AWS SDK uploader default: below it a non-seekable
// body is sent as a single PutObject.
const multipartPartSize = 5 * 1024 * 1024

// countResultRows streams the result instead of holding it in memory, which the
// multipart case is too large for.
func countResultRows(t *testing.T, h *harness, id string) int {
	t.Helper()
	reader, _, err := h.svc.DownloadResult(context.Background(), id, 0, 0)
	if err != nil {
		t.Fatalf("DownloadResult: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()

	n := 0
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			n++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read result: %v", err)
	}
	return n
}

// TestRedis_MultiNode covers what a single-node in-memory store cannot: two
// instances sharing one Redis, an idempotency key resolving across them, a
// subscription on the non-owner receiving events (I2), and a stop request
// crossing instances.
func TestRedis_MultiNode(t *testing.T) {
	redisAddr := startRedis(t)
	dsn := startPostgres(t)

	a := newHarness(t, harnessOptions{instanceID: "node-a", redisAddr: redisAddr, databases: pgDatabases(dsn)})
	b := newHarness(t, harnessOptions{instanceID: "node-b", redisAddr: redisAddr, databases: pgDatabases(dsn)})

	// I3: the same key on a different instance returns the same query.
	opts := domain.QueryOptions{Mode: "sync", IdempotencyKey: "shared-key"}
	first, err := a.svc.StartQuery(context.Background(), "pg", "SELECT 1", opts)
	if err != nil {
		t.Fatalf("StartQuery on node-a: %v", err)
	}
	second, err := b.svc.StartQuery(context.Background(), "pg", "SELECT 1", opts)
	if err != nil {
		t.Fatalf("StartQuery on node-b: %v", err)
	}
	if first.ID != second.ID {
		t.Errorf("idempotency across instances: %s vs %s", first.ID, second.ID)
	}

	// I2: the result is readable from the instance that did not run it.
	if _, err := b.svc.GetQueryStatus(context.Background(), first.ID); err != nil {
		t.Errorf("reading node-a's query from node-b: %v", err)
	}

	// A long query owned by node-a, watched and stopped through node-b.
	long, err := a.svc.StartQuery(context.Background(), "pg", "SELECT pg_sleep(30)",
		domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("StartQuery (long): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events, err := b.svc.WatchQuery(ctx, long.ID)
	if err != nil {
		t.Fatalf("WatchQuery on node-b: %v", err)
	}

	if err := b.svc.StopQuery(context.Background(), long.ID); err != nil {
		t.Fatalf("StopQuery through node-b: %v", err)
	}

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("the subscription closed without a terminal event")
			}
			if ev.State.IsTerminal() {
				if ev.State != domain.StateCanceled {
					t.Errorf("terminal state = %s, want CANCELED", ev.State)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("no terminal event reached node-b")
		}
	}
}

// TestRedis_OwnerLost covers the lease path end to end: a record owned by an
// instance that never heartbeats is failed once its lease expires, and a
// terminal record is left alone.
func TestRedis_OwnerLost(t *testing.T) {
	redisAddr := startRedis(t)
	dsn := startPostgres(t)

	h := newHarness(t, harnessOptions{instanceID: "reaper", redisAddr: redisAddr, databases: pgDatabases(dsn)})

	orphan := &domain.QueryRecord{
		ID:              "orphan-1",
		DatabaseID:      "pg",
		SQL:             "SELECT 1",
		State:           domain.StateRunning,
		OwnerInstanceID: "gone",
		CreatedAt:       time.Now().Add(-time.Hour),
		Options:         domain.QueryOptions{ResultTTL: time.Hour},
	}
	if err := h.ms.PutQuery(context.Background(), orphan); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	done := &domain.QueryRecord{
		ID:              "done-1",
		DatabaseID:      "pg",
		State:           domain.StateSucceeded,
		OwnerInstanceID: "gone",
		CreatedAt:       time.Now().Add(-time.Hour),
		FinishedAt:      time.Now(),
		Options:         domain.QueryOptions{ResultTTL: time.Hour},
		Result:          &domain.ResultRef{Backend: "fs", Locator: "x", Format: "jsonl"},
	}
	if err := h.ms.PutQuery(context.Background(), done); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		rec, err := h.ms.GetQuery(context.Background(), "orphan-1")
		if err != nil {
			t.Fatalf("GetQuery: %v", err)
		}
		if rec.State == domain.StateFailed {
			if rec.Error == nil || rec.Error.Code != domain.ErrCodeOwnerLost {
				t.Errorf("error = %+v, want OWNER_LOST", rec.Error)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the orphan was never reaped, state = %s", rec.State)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The terminal record must survive the same pass with its result intact.
	still, err := h.ms.GetQuery(context.Background(), "done-1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if still.State != domain.StateSucceeded || still.Result == nil {
		t.Errorf("terminal record was rewritten: state = %s result = %+v", still.State, still.Result)
	}
}

// TestRedis_HeartbeatKeepsTerminalWrites is the regression for the lost update
// that motivated moving leases into their own keys. The heartbeat used to
// read-modify-write the record, so the sequence "heartbeat reads RUNNING ->
// terminal write lands -> heartbeat writes RUNNING back" undid a finished
// query. That interleaving only happens under real concurrency: run
// sequentially, the buggy implementation passed this test.
func TestRedis_HeartbeatKeepsTerminalWrites(t *testing.T) {
	redisAddr := startRedis(t)
	store := state.NewRedisMetaStore(redisAddr, "", 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	ctx := context.Background()
	const rounds = 50

	for round := range rounds {
		id := fmt.Sprintf("q%d", round)
		rec := &domain.QueryRecord{
			ID: id, DatabaseID: "pg", State: domain.StateRunning,
			OwnerInstanceID: "node-a", CreatedAt: time.Now(),
			Options: domain.QueryOptions{ResultTTL: time.Hour},
		}
		if err := store.PutQuery(ctx, rec); err != nil {
			t.Fatalf("PutQuery: %v", err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		var hbErr atomic.Pointer[error]
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := store.Heartbeat(ctx, "node-a", []string{id}, 5*time.Second); err != nil {
					hbErr.Store(&err)
					return
				}
			}
		})

		// Let the heartbeat loop get going, land the terminal write in the
		// middle of it, then keep beating for a while afterwards.
		time.Sleep(2 * time.Millisecond)
		done := *rec
		done.State = domain.StateSucceeded
		done.FinishedAt = time.Now()
		done.Result = &domain.ResultRef{Backend: "fs", Locator: "x", Format: "jsonl"}
		if err := store.PutQuery(ctx, &done); err != nil {
			t.Fatalf("PutQuery terminal: %v", err)
		}
		time.Sleep(5 * time.Millisecond)

		close(stop)
		wg.Wait()
		if err := hbErr.Load(); err != nil {
			t.Fatalf("Heartbeat: %v", *err)
		}

		got, err := store.GetQuery(ctx, id)
		if err != nil {
			t.Fatalf("GetQuery: %v", err)
		}
		if got.State != domain.StateSucceeded || got.Result == nil {
			t.Fatalf("round %d: a heartbeat clobbered the terminal write: state = %s result = %+v",
				round, got.State, got.Result)
		}

		n, err := store.CountInFlight(ctx, "node-a")
		if err != nil {
			t.Fatalf("CountInFlight: %v", err)
		}
		if n != 0 {
			t.Fatalf("round %d: CountInFlight = %d after the terminal write, want 0", round, n)
		}
	}
}

// TestRedis_TryLockIsExclusive covers the cluster lock that keeps GC on one
// instance at a time, against real Redis rather than the miniredis emulation of
// SET NX. It does not run GC itself: collectGarbage is unexported and its first
// tick is a minute away.
func TestRedis_TryLockIsExclusive(t *testing.T) {
	redisAddr := startRedis(t)
	store := state.NewRedisMetaStore(redisAddr, "", 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	ctx := context.Background()
	first, err := store.TryLock(ctx, "gc", 30*time.Second)
	if err != nil || !first {
		t.Fatalf("first TryLock = (%v, %v), want (true, nil)", first, err)
	}
	second, err := store.TryLock(ctx, "gc", 30*time.Second)
	if err != nil {
		t.Fatalf("second TryLock: %v", err)
	}
	if second {
		t.Error("two instances acquired the GC lock at once")
	}

	// A second store, which is what a second instance really is.
	other := state.NewRedisMetaStore(redisAddr, "", 0)
	t.Cleanup(func() {
		if err := other.Close(); err != nil {
			t.Errorf("close other store: %v", err)
		}
	})
	held, err := other.TryLock(ctx, "gc", 30*time.Second)
	if err != nil {
		t.Fatalf("TryLock from a second instance: %v", err)
	}
	if held {
		t.Error("a second instance took a lock the first one holds")
	}

	// The lock has to expire on its own, or one crashed instance would stop GC
	// across the cluster for good.
	shortLock, err := store.TryLock(ctx, "gc-short", 300*time.Millisecond)
	if err != nil || !shortLock {
		t.Fatalf("TryLock(gc-short) = (%v, %v), want (true, nil)", shortLock, err)
	}
	time.Sleep(500 * time.Millisecond)
	again, err := other.TryLock(ctx, "gc-short", time.Second)
	if err != nil {
		t.Fatalf("TryLock after expiry: %v", err)
	}
	if !again {
		t.Error("the lock did not expire, so a crashed instance would block GC for good")
	}
}

// TestRedis_ReadOnlyGuard confirms the statement guard is in force against a
// real database: a rejected write must never reach it.
func TestRedis_ReadOnlyGuard(t *testing.T) {
	redisAddr := startRedis(t)
	dsn := startPostgres(t)

	h := newHarness(t, harnessOptions{instanceID: "guard", redisAddr: redisAddr, databases: pgDatabases(dsn)})

	_, err := h.svc.StartQuery(context.Background(), "pg", "DELETE FROM users", domain.QueryOptions{})
	if _, ok := errors.AsType[domain.ValidationError](err); !ok {
		t.Fatalf("StartQuery error = %v, want domain.ValidationError", err)
	}

	rec, err := h.svc.StartQuery(context.Background(), "pg", "SELECT count(*) AS n FROM users",
		domain.QueryOptions{Mode: "sync"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	rows := readResult(t, h, rec.ID)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one", rows)
	}
	if got := fmt.Sprint(rows[0]["n"]); got != "2" {
		t.Errorf("count = %s, want 2 (the rejected DELETE must not have run)", got)
	}
}
