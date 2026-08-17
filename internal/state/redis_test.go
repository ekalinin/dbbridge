package state

import (
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"

	"github.com/alicebob/miniredis/v2"
)

func newRedisStore(t *testing.T) (*miniredis.Miniredis, *RedisMetaStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	store := NewRedisMetaStore(mr.Addr(), "", 0)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return mr, store
}

func runningRecord(id string) *domain.QueryRecord {
	return &domain.QueryRecord{
		ID:              id,
		DatabaseID:      "db1",
		SQL:             "SELECT 1",
		State:           domain.StateRunning,
		OwnerInstanceID: "inst-1",
		CreatedAt:       time.Now(),
		Options:         domain.QueryOptions{ResultTTL: time.Hour},
	}
}

// TestRedisHeartbeatDoesNotRewriteRecord pins the fix for the lost-update race:
// a heartbeat must touch only the lease key, never the record, otherwise it can
// overwrite a terminal state written concurrently by run().
func TestRedisHeartbeatDoesNotRewriteRecord(t *testing.T) {
	mr, store := newRedisStore(t)
	ctx := t.Context()

	if err := store.PutQuery(ctx, runningRecord("q1")); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}
	before, err := mr.Get("dbbridge:query:q1")
	if err != nil {
		t.Fatalf("seed read: %v", err)
	}

	if err := store.Heartbeat(ctx, "inst-1", []string{"q1"}, 5*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	after, err := mr.Get("dbbridge:query:q1")
	if err != nil {
		t.Fatalf("read after heartbeat: %v", err)
	}
	if after != before {
		t.Fatalf("heartbeat rewrote the query record:\nbefore=%s\nafter =%s", before, after)
	}
	if !mr.Exists("dbbridge:lease:q1") {
		t.Fatal("lease key was not written")
	}
}

// TestRedisGetQueryDerivesLeaseDeadline checks that LeaseDeadline is still
// exposed through the API after moving it out of the stored record.
func TestRedisGetQueryDerivesLeaseDeadline(t *testing.T) {
	_, store := newRedisStore(t)
	ctx := t.Context()

	if err := store.PutQuery(ctx, runningRecord("q1")); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	got, err := store.GetQuery(ctx, "q1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if !got.LeaseDeadline.IsZero() {
		t.Errorf("LeaseDeadline = %v before any heartbeat, want zero", got.LeaseDeadline)
	}

	if err := store.Heartbeat(ctx, "inst-1", []string{"q1"}, 5*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	got, err = store.GetQuery(ctx, "q1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.LeaseDeadline.IsZero() {
		t.Fatal("LeaseDeadline is zero after a heartbeat")
	}
	if d := time.Until(got.LeaseDeadline); d <= 0 || d > 5*time.Second {
		t.Errorf("LeaseDeadline is %v away, want within (0s, 5s]", d)
	}
}

func TestRedisListStaleQueriesFollowsLease(t *testing.T) {
	mr, store := newRedisStore(t)
	ctx := t.Context()

	if err := store.PutQuery(ctx, runningRecord("q1")); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}
	if err := store.Heartbeat(ctx, "inst-1", []string{"q1"}, 5*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	stale, err := store.ListStaleQueries(ctx)
	if err != nil {
		t.Fatalf("ListStaleQueries: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("ListStaleQueries = %v right after a heartbeat, want none", stale)
	}

	mr.FastForward(6 * time.Second)

	stale, err = store.ListStaleQueries(ctx)
	if err != nil {
		t.Fatalf("ListStaleQueries: %v", err)
	}
	if len(stale) != 1 || stale[0] != "q1" {
		t.Fatalf("ListStaleQueries = %v after lease expiry, want [q1]", stale)
	}
}

// TestRedisTerminalWriteDropsLease guards the reverse direction: once a query is
// terminal it must disappear from the owner's in-flight set (I5) and lose its
// lease so the reaper never revisits it.
func TestRedisTerminalWriteDropsLease(t *testing.T) {
	mr, store := newRedisStore(t)
	ctx := t.Context()

	rec := runningRecord("q1")
	if err := store.PutQuery(ctx, rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}
	if err := store.Heartbeat(ctx, "inst-1", []string{"q1"}, 5*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	rec.State = domain.StateSucceeded
	rec.FinishedAt = time.Now()
	if err := store.PutQuery(ctx, rec); err != nil {
		t.Fatalf("PutQuery terminal: %v", err)
	}

	if mr.Exists("dbbridge:lease:q1") {
		t.Error("lease key survived the terminal write")
	}
	n, err := store.CountInFlight(ctx, "inst-1")
	if err != nil {
		t.Fatalf("CountInFlight: %v", err)
	}
	if n != 0 {
		t.Errorf("CountInFlight = %d after terminal write, want 0", n)
	}
}

func TestRedisUpdateQueryIfState(t *testing.T) {
	_, store := newRedisStore(t)
	ctx := t.Context()

	rec := runningRecord("q1")
	if err := store.PutQuery(ctx, rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	// Matching state: the write goes through.
	next := *rec
	next.State = domain.StateSucceeded
	next.FinishedAt = time.Now()
	ok, err := store.UpdateQueryIfState(ctx, &next, domain.StatePending, domain.StateRunning)
	if err != nil {
		t.Fatalf("UpdateQueryIfState: %v", err)
	}
	if !ok {
		t.Fatal("UpdateQueryIfState reported no write for a matching state")
	}

	// The record is terminal now, so the same conditional write is refused —
	// this is what stops the reaper from resurrecting a finished query.
	reaped := *rec
	reaped.State = domain.StateFailed
	ok, err = store.UpdateQueryIfState(ctx, &reaped, domain.StatePending, domain.StateRunning)
	if err != nil {
		t.Fatalf("UpdateQueryIfState (second): %v", err)
	}
	if ok {
		t.Fatal("UpdateQueryIfState overwrote a terminal record")
	}

	got, err := store.GetQuery(ctx, "q1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StateSucceeded {
		t.Errorf("state = %s, want SUCCEEDED", got.State)
	}
}

func TestRedisIdempotencyLifecycle(t *testing.T) {
	mr, store := newRedisStore(t)
	ctx := t.Context()

	id, acquired, err := store.AcquireIdempotency(ctx, "db1", "k", "q1", time.Minute)
	if err != nil || !acquired || id != "q1" {
		t.Fatalf("AcquireIdempotency = (%q, %v, %v), want (q1, true, nil)", id, acquired, err)
	}

	id, acquired, err = store.AcquireIdempotency(ctx, "db1", "k", "q2", time.Minute)
	if err != nil || acquired || id != "q1" {
		t.Fatalf("duplicate AcquireIdempotency = (%q, %v, %v), want (q1, false, nil)", id, acquired, err)
	}

	// A foreign query must not be able to free the key.
	if err := store.ReleaseIdempotency(ctx, "db1", "k", "q2"); err != nil {
		t.Fatalf("ReleaseIdempotency (foreign): %v", err)
	}
	if !mr.Exists("dbbridge:idempotency:db1:k") {
		t.Fatal("a foreign query released the idempotency key")
	}

	// Retention starts at FinishedAt, so the owner re-arms the TTL on finish.
	if err := store.RefreshIdempotency(ctx, "db1", "k", "q1", time.Hour); err != nil {
		t.Fatalf("RefreshIdempotency: %v", err)
	}
	if ttl := mr.TTL("dbbridge:idempotency:db1:k"); ttl <= time.Minute {
		t.Errorf("TTL after refresh = %v, want > 1m", ttl)
	}

	if err := store.ReleaseIdempotency(ctx, "db1", "k", "q1"); err != nil {
		t.Fatalf("ReleaseIdempotency: %v", err)
	}
	if mr.Exists("dbbridge:idempotency:db1:k") {
		t.Fatal("the owning query failed to release the idempotency key")
	}
}

func TestRedisTryLock(t *testing.T) {
	mr, store := newRedisStore(t)
	ctx := t.Context()

	ok, err := store.TryLock(ctx, "gc", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("first TryLock = (%v, %v), want (true, nil)", ok, err)
	}

	ok, err = store.TryLock(ctx, "gc", 30*time.Second)
	if err != nil || ok {
		t.Fatalf("second TryLock = (%v, %v), want (false, nil)", ok, err)
	}

	mr.FastForward(31 * time.Second)

	ok, err = store.TryLock(ctx, "gc", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("TryLock after expiry = (%v, %v), want (true, nil)", ok, err)
	}
}
