package state

import (
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

func TestMemoryMetaStoreQueries(t *testing.T) {
	ms := NewMemoryMetaStore()
	defer ms.Close()

	ctx := t.Context()
	rec := &domain.QueryRecord{
		ID:         "test-query",
		DatabaseID: "pg_test",
		SQL:        "SELECT 1",
		State:      domain.StatePending,
		CreatedAt:  time.Now(),
	}

	// Put
	if err := ms.PutQuery(ctx, rec); err != nil {
		t.Fatalf("failed to put query: %v", err)
	}

	// Get
	got, err := ms.GetQuery(ctx, "test-query")
	if err != nil {
		t.Fatalf("failed to get query: %v", err)
	}
	if got.SQL != rec.SQL {
		t.Errorf("expected SQL %q; got %q", rec.SQL, got.SQL)
	}

	// Update
	rec.State = domain.StateRunning
	if err := ms.UpdateQuery(ctx, rec); err != nil {
		t.Fatalf("failed to update query: %v", err)
	}

	got, err = ms.GetQuery(ctx, "test-query")
	if err != nil {
		t.Fatalf("failed to get query after update: %v", err)
	}
	if got.State != domain.StateRunning {
		t.Errorf("expected state RUNNING; got %q", got.State)
	}

	// Delete
	if err := ms.DeleteQuery(ctx, "test-query"); err != nil {
		t.Fatalf("failed to delete query: %v", err)
	}

	_, err = ms.GetQuery(ctx, "test-query")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound; got %v", err)
	}
}

func TestMemoryMetaStoreListByInstanceAndDatabasesSeen(t *testing.T) {
	ms := NewMemoryMetaStore()
	defer ms.Close()

	ctx := t.Context()
	put := func(id, dbID, owner string, st domain.QueryState) {
		if err := ms.PutQuery(ctx, &domain.QueryRecord{
			ID: id, DatabaseID: dbID, OwnerInstanceID: owner, State: st, CreatedAt: time.Now(),
		}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}

	put("q1", "pg_main", "inst-a", domain.StateRunning)
	put("q2", "pg_main", "inst-a", domain.StatePending)
	put("q3", "ch_logs", "inst-a", domain.StateSucceeded) // terminal -> excluded from ListByInstance
	put("q4", "pg_main", "inst-b", domain.StateRunning)   // owned by another instance

	owned, err := ms.ListByInstance(ctx, "inst-a")
	if err != nil {
		t.Fatalf("ListByInstance: %v", err)
	}
	if len(owned) != 2 {
		t.Errorf("ListByInstance(inst-a) = %v, want 2 active queries (q1,q2)", owned)
	}

	dbs, err := ms.ListDatabasesSeen(ctx)
	if err != nil {
		t.Fatalf("ListDatabasesSeen: %v", err)
	}
	if len(dbs) != 2 {
		t.Errorf("ListDatabasesSeen = %v, want 2 distinct (pg_main, ch_logs)", dbs)
	}
}

func TestMemoryMetaStoreIdempotency(t *testing.T) {
	ms := NewMemoryMetaStore()
	defer ms.Close()

	ctx := t.Context()

	// Acquire first time
	qid, acquired, err := ms.AcquireIdempotency(ctx, "pg_test", "idem-key", "query-1", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acquired {
		t.Error("expected acquired=true on first call")
	}
	if qid != "query-1" {
		t.Errorf("expected qid 'query-1'; got %q", qid)
	}

	// Try to acquire again
	qid2, acquired2, err := ms.AcquireIdempotency(ctx, "pg_test", "idem-key", "query-2", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if acquired2 {
		t.Error("expected acquired=false on duplicate call")
	}
	if qid2 != "query-1" {
		t.Errorf("expected returned qid of existing query 'query-1'; got %q", qid2)
	}
}

// TestMemoryMetaStoreLeaseAndFencing mirrors the Redis behaviour the manager
// relies on: heartbeats never rewrite the record, and a conditional update is
// refused once the record is terminal.
func TestMemoryMetaStoreLeaseAndFencing(t *testing.T) {
	ms := NewMemoryMetaStore()
	defer ms.Close()

	ctx := t.Context()
	rec := &domain.QueryRecord{
		ID: "q1", DatabaseID: "pg", OwnerInstanceID: "inst-1",
		State: domain.StateRunning, CreatedAt: time.Now(),
	}
	if err := ms.PutQuery(ctx, rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	stale, err := ms.ListStaleQueries(ctx)
	if err != nil {
		t.Fatalf("ListStaleQueries: %v", err)
	}
	if len(stale) != 1 {
		t.Errorf("ListStaleQueries = %v before any heartbeat, want [q1]", stale)
	}

	if err := ms.Heartbeat(ctx, "inst-1", []string{"q1"}, time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	stale, err = ms.ListStaleQueries(ctx)
	if err != nil {
		t.Fatalf("ListStaleQueries: %v", err)
	}
	if len(stale) != 0 {
		t.Errorf("ListStaleQueries = %v after a heartbeat, want none", stale)
	}

	got, err := ms.GetQuery(ctx, "q1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.LeaseDeadline.IsZero() {
		t.Error("LeaseDeadline is zero after a heartbeat")
	}

	done := *rec
	done.State = domain.StateSucceeded
	done.FinishedAt = time.Now()
	ok, err := ms.UpdateQueryIfState(ctx, &done, domain.StatePending, domain.StateRunning)
	if err != nil || !ok {
		t.Fatalf("UpdateQueryIfState = (%v, %v), want (true, nil)", ok, err)
	}

	reaped := *rec
	reaped.State = domain.StateFailed
	ok, err = ms.UpdateQueryIfState(ctx, &reaped, domain.StatePending, domain.StateRunning)
	if err != nil {
		t.Fatalf("UpdateQueryIfState (second): %v", err)
	}
	if ok {
		t.Fatal("UpdateQueryIfState overwrote a terminal record")
	}
}

func TestMemoryMetaStoreTryLock(t *testing.T) {
	ms := NewMemoryMetaStore()
	defer ms.Close()

	ctx := t.Context()
	if ok, err := ms.TryLock(ctx, "gc", time.Minute); err != nil || !ok {
		t.Fatalf("first TryLock = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := ms.TryLock(ctx, "gc", time.Minute); err != nil || ok {
		t.Fatalf("second TryLock = (%v, %v), want (false, nil)", ok, err)
	}
}
