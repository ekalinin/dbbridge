package state

import (
	"context"
	"errors"
	"time"

	"dbbridge/internal/core/domain"
)

var (
	ErrNotFound = errors.New("query not found")
)

// ControlType specifies types of cross-instance control messages.
type ControlType string

const (
	ControlStopQuery ControlType = "STOP_QUERY"
)

// ControlMsg represents a payload exchanged between nodes via Pub/Sub.
type ControlMsg struct {
	Type     ControlType `json:"type"`
	QueryID  string      `json:"query_id"`
	SenderID string      `json:"sender_id"`
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

	// AcquireIdempotency tries to lock a key for a dbID.
	// Returns (existingQueryID, acquired=true, nil) if successfully acquired,
	// or (existingQueryID, acquired=false, nil) if already locked.
	AcquireIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) (string, bool, error)

	// Heartbeat registers the node's presence and updates its leased queries.
	Heartbeat(ctx context.Context, instanceID string, ownedQueryIDs []string, ttl time.Duration) error

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
	// owner instance is no longer alive (its heartbeat/lease key has expired).
	ListStaleQueries(ctx context.Context) ([]string, error)

	// DeleteQuery deletes query metadata.
	DeleteQuery(ctx context.Context, id string) error

	// Close cleans up connections.
	Close() error
}
