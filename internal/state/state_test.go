package state

import (
	"testing"
	"time"

	"dbbridge/internal/core/domain"
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
