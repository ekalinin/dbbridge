package state

import (
	"context"
	"errors"
	"slices"
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

// terminalRecord builds a finished query whose retention window has already
// elapsed, which is what ListExpiredQueries looks for.
func terminalRecord(id string, finishedAgo, ttl time.Duration) *domain.QueryRecord {
	rec := runningRecord(id)
	rec.State = domain.StateSucceeded
	rec.FinishedAt = time.Now().Add(-finishedAgo)
	rec.Options.ResultTTL = ttl
	return rec
}

// TestRedisListByInstance covers the list recoverOrphans reads at startup: the
// queries this instance still owns in the MetaStore but is no longer running.
// A terminal query has to drop out of it, or a restarted node would report
// itself busy for ever and never reach can_be_stopped=true (I5).
func TestRedisListByInstance(t *testing.T) {
	_, store := newRedisStore(t)
	ctx := t.Context()

	for _, rec := range []*domain.QueryRecord{runningRecord("own-1"), runningRecord("own-2")} {
		if err := store.PutQuery(ctx, rec); err != nil {
			t.Fatalf("PutQuery: %v", err)
		}
	}
	other := runningRecord("other-1")
	other.OwnerInstanceID = "inst-2"
	if err := store.PutQuery(ctx, other); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}

	ids, err := store.ListByInstance(ctx, "inst-1")
	if err != nil {
		t.Fatalf("ListByInstance: %v", err)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"own-1", "own-2"}) {
		t.Fatalf("ListByInstance = %v, want [own-1 own-2]", ids)
	}

	if n, err := store.CountInFlight(ctx, "inst-1"); err != nil || n != 2 {
		t.Fatalf("CountInFlight = %d, %v; want 2, nil", n, err)
	}

	// Finishing one drops it from the owner's active set.
	done := runningRecord("own-1")
	done.State = domain.StateSucceeded
	done.FinishedAt = time.Now()
	if err := store.PutQuery(ctx, done); err != nil {
		t.Fatalf("PutQuery terminal: %v", err)
	}
	ids, err = store.ListByInstance(ctx, "inst-1")
	if err != nil {
		t.Fatalf("ListByInstance after terminal: %v", err)
	}
	if !slices.Equal(ids, []string{"own-2"}) {
		t.Fatalf("ListByInstance after terminal = %v, want [own-2]", ids)
	}
}

// TestRedisListDatabasesSeen pins the behaviour that separates this store from
// the in-memory one: the set is persistent, so a database keeps being reported
// after the query that named it is gone. That is what lets ListDatabases surface
// a database dropped from the config while its results are still downloadable.
func TestRedisListDatabasesSeen(t *testing.T) {
	_, store := newRedisStore(t)
	ctx := t.Context()

	first := runningRecord("q1")
	second := runningRecord("q2")
	second.DatabaseID = "db2"
	for _, rec := range []*domain.QueryRecord{first, second} {
		if err := store.PutQuery(ctx, rec); err != nil {
			t.Fatalf("PutQuery: %v", err)
		}
	}

	seen, err := store.ListDatabasesSeen(ctx)
	if err != nil {
		t.Fatalf("ListDatabasesSeen: %v", err)
	}
	slices.Sort(seen)
	if !slices.Equal(seen, []string{"db1", "db2"}) {
		t.Fatalf("ListDatabasesSeen = %v, want [db1 db2]", seen)
	}

	if err := store.DeleteQuery(ctx, "q2"); err != nil {
		t.Fatalf("DeleteQuery: %v", err)
	}
	seen, err = store.ListDatabasesSeen(ctx)
	if err != nil {
		t.Fatalf("ListDatabasesSeen after delete: %v", err)
	}
	slices.Sort(seen)
	if !slices.Equal(seen, []string{"db1", "db2"}) {
		t.Fatalf("ListDatabasesSeen = %v after deleting the only db2 query, want it retained", seen)
	}
}

// TestRedisListExpiredQueries covers the scan GC runs. Expiry is measured from
// FinishedAt plus the record's own ResultTTL, so a running query and a recently
// finished one both have to stay out of the result.
func TestRedisListExpiredQueries(t *testing.T) {
	_, store := newRedisStore(t)
	ctx := t.Context()

	records := []*domain.QueryRecord{
		terminalRecord("gone", time.Hour, time.Minute),
		terminalRecord("fresh", time.Minute, time.Hour),
		runningRecord("live"),
	}
	// A terminal record with no FinishedAt cannot have its retention measured,
	// so it must not be swept on a zero timestamp.
	noFinish := runningRecord("no-finish")
	noFinish.State = domain.StateFailed
	records = append(records, noFinish)

	for _, rec := range records {
		if err := store.PutQuery(ctx, rec); err != nil {
			t.Fatalf("PutQuery %s: %v", rec.ID, err)
		}
	}

	expired, err := store.ListExpiredQueries(ctx)
	if err != nil {
		t.Fatalf("ListExpiredQueries: %v", err)
	}
	if !slices.Equal(expired, []string{"gone"}) {
		t.Fatalf("ListExpiredQueries = %v, want [gone]", expired)
	}
}

// TestRedisDeleteQuery covers the removal GC performs: the record, its lease and
// its place in the owner's active set all go, and the query stops being readable.
func TestRedisDeleteQuery(t *testing.T) {
	mr, store := newRedisStore(t)
	ctx := t.Context()

	if err := store.PutQuery(ctx, runningRecord("q1")); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}
	if err := store.Heartbeat(ctx, "inst-1", []string{"q1"}, 5*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if err := store.DeleteQuery(ctx, "q1"); err != nil {
		t.Fatalf("DeleteQuery: %v", err)
	}

	if _, err := store.GetQuery(ctx, "q1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetQuery after delete: %v, want ErrNotFound", err)
	}
	if mr.Exists("dbbridge:lease:q1") {
		t.Error("the lease key outlived the query it belonged to")
	}
	if n, err := store.CountInFlight(ctx, "inst-1"); err != nil || n != 0 {
		t.Fatalf("CountInFlight = %d, %v; want 0, nil", n, err)
	}

	// Deleting a query that is already gone is what a second GC pass does; it
	// must not fail.
	if err := store.DeleteQuery(ctx, "q1"); err != nil {
		t.Fatalf("DeleteQuery on a missing record: %v", err)
	}
}

// TestRedisUpdateQuery covers the unconditional write GC uses to mark a record
// EXPIRED. Unlike the memory store it does not require the record to exist,
// which is worth pinning so a change to either implementation is deliberate.
func TestRedisUpdateQuery(t *testing.T) {
	_, store := newRedisStore(t)
	ctx := t.Context()

	if err := store.PutQuery(ctx, runningRecord("q1")); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}
	expired := runningRecord("q1")
	expired.State = domain.StateExpired
	expired.FinishedAt = time.Now()
	if err := store.UpdateQuery(ctx, expired); err != nil {
		t.Fatalf("UpdateQuery: %v", err)
	}

	got, err := store.GetQuery(ctx, "q1")
	if err != nil {
		t.Fatalf("GetQuery: %v", err)
	}
	if got.State != domain.StateExpired {
		t.Fatalf("state = %s, want EXPIRED", got.State)
	}
}

// TestRedisControlRoundTrip covers the Pub/Sub channel §5.5 builds cross-instance
// stop and query events on. Without it a subscription opened through any
// instance other than the owner never fires, which breaks I2 for WebSocket and
// WatchQuery.
func TestRedisControlRoundTrip(t *testing.T) {
	_, store := newRedisStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := store.SubscribeControl(ctx)
	if err != nil {
		t.Fatalf("SubscribeControl: %v", err)
	}

	sent := ControlMsg{
		Type:     ControlQueryEvent,
		QueryID:  "q1",
		SenderID: "inst-2",
		Event: &QueryEventPayload{
			State: string(domain.StateSucceeded),
			Stats: domain.QueryStats{RowsRead: 7, BytesWritten: 42},
		},
	}
	// The subscription is established asynchronously by go-redis, so publish
	// until one lands rather than racing a single send.
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := store.PublishControl(ctx, sent); err != nil {
			t.Fatalf("PublishControl: %v", err)
		}
		select {
		case got := <-ch:
			if got.Type != sent.Type || got.QueryID != sent.QueryID || got.SenderID != sent.SenderID {
				t.Fatalf("received %+v, want %+v", got, sent)
			}
			if got.Event == nil {
				t.Fatal("the event payload did not survive the round trip")
			}
			if got.Event.State != sent.Event.State || got.Event.Stats.RowsRead != 7 {
				t.Fatalf("event = %+v, want state=%s rows_read=7", got.Event, sent.Event.State)
			}
			return
		case <-ticker.C:
		case <-deadline:
			t.Fatal("no control message arrived on the subscription")
		}
	}
}

// TestRedisSubscribeControlClosesOnContext keeps the subscription tied to its
// context: controlWorker stops by cancelling, and a channel left open would keep
// a goroutine and a Redis connection alive for the life of the process.
func TestRedisSubscribeControlClosesOnContext(t *testing.T) {
	_, store := newRedisStore(t)
	ctx, cancel := context.WithCancel(t.Context())

	ch, err := store.SubscribeControl(ctx)
	if err != nil {
		t.Fatalf("SubscribeControl: %v", err)
	}
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			// A message queued before the cancel is fine; the close still has to
			// follow it.
			select {
			case _, ok := <-ch:
				if ok {
					t.Fatal("the control channel kept delivering after its context was canceled")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the control channel was not closed after its context was canceled")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the control channel was not closed after its context was canceled")
	}
}
