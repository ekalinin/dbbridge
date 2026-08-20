package state

import (
	"context"
	"errors"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

var (
	ErrNotFound = errors.New("query not found")
)

// ControlType specifies types of cross-instance control messages.
type ControlType string

const (
	ControlStopQuery ControlType = "STOP_QUERY"
	// ControlQueryEvent carries a state or progress change to the instances
	// that hold subscriptions for a query they do not own. Watchers live in a
	// per-process map, so without this a subscription opened through any other
	// instance never received an event (I2).
	ControlQueryEvent ControlType = "QUERY_EVENT"
)

// QueryEventPayload is the body of a ControlQueryEvent.
type QueryEventPayload struct {
	State string             `json:"state"`
	Stats domain.QueryStats  `json:"stats"`
	Error *domain.QueryError `json:"error,omitempty"`
}

// ControlMsg represents a payload exchanged between nodes via Pub/Sub.
type ControlMsg struct {
	Type     ControlType        `json:"type"`
	QueryID  string             `json:"query_id"`
	SenderID string             `json:"sender_id"`
	Event    *QueryEventPayload `json:"event,omitempty"`
}

// MetaStore defines the persistence layer for tracking query execution metadata,
// managing cluster coordination (heartbeats), cross-instance cancellations, and idempotency.
type MetaStore interface {
	// PutQuery creates or overwrites a query record.
	PutQuery(ctx context.Context, record *domain.QueryRecord) error

	// GetQuery retrieves a query record by its ID.
	GetQuery(ctx context.Context, id string) (*domain.QueryRecord, error)

	// UpdateQuery updates an existing query record.
	UpdateQuery(ctx context.Context, record *domain.QueryRecord) error

	// UpdateQueryIfState writes the record only while the stored state is one of
	// expected, and reports whether the write happened. It is the fencing
	// primitive that keeps a reaper or a late writer from resurrecting a query
	// that has already reached a terminal state.
	UpdateQueryIfState(ctx context.Context, record *domain.QueryRecord, expected ...domain.QueryState) (bool, error)

	// AcquireIdempotency tries to lock a key for a dbID.
	// Returns (existingQueryID, acquired=true, nil) if successfully acquired,
	// or (existingQueryID, acquired=false, nil) if already locked.
	AcquireIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) (string, bool, error)

	// ReleaseIdempotency drops a key previously acquired for queryID. It is a
	// no-op when the key now points at a different query, so a rollback can
	// never free somebody else's key.
	ReleaseIdempotency(ctx context.Context, dbID, key, queryID string) error

	// RefreshIdempotency re-arms the TTL of a key owned by queryID. The result
	// retention window starts at FinishedAt, so the key has to be extended when
	// the query finishes to stay valid for exactly as long as the result (I3).
	RefreshIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) error

	// Heartbeat registers the node's presence and refreshes the lease of every
	// owned query. Leases live in their own keys: refreshing one must never
	// rewrite the query record itself, or a concurrent terminal write is lost.
	Heartbeat(ctx context.Context, instanceID string, ownedQueryIDs []string, ttl time.Duration) error

	// TryLock acquires a cluster-wide named lock for ttl. It returns false when
	// the lock is already held. Used to keep periodic cluster-wide work (GC)
	// running on a single instance at a time.
	TryLock(ctx context.Context, name string, ttl time.Duration) (bool, error)

	// PublishControl sends a control message to all nodes.
	PublishControl(ctx context.Context, msg ControlMsg) error

	// SubscribeControl listens for incoming control messages.
	SubscribeControl(ctx context.Context) (<-chan ControlMsg, error)

	// CountInFlight returns the number of active (pending/running) queries owned by an instance.
	CountInFlight(ctx context.Context, instanceID string) (int, error)

	// ListByInstance returns the IDs of active (non-terminal) queries owned by an instance.
	ListByInstance(ctx context.Context, instanceID string) ([]string, error)

	// ListDatabasesSeen returns the IDs of databases that have had at least one query submitted.
	ListDatabasesSeen(ctx context.Context) ([]string, error)

	// ListExpiredQueries returns IDs of queries whose ResultTTL has expired (based on finished_at + result_ttl).
	ListExpiredQueries(ctx context.Context) ([]string, error)

	// ListStaleQueries returns IDs of non-terminal (PENDING/RUNNING) queries whose
	// lease has expired, meaning no live owner is heartbeating them any more.
	ListStaleQueries(ctx context.Context) ([]string, error)

	// DeleteQuery deletes query metadata.
	DeleteQuery(ctx context.Context, id string) error

	// Close cleans up connections.
	Close() error
}
