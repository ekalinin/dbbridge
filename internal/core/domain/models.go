package domain

import (
	"fmt"
	"time"
)

// QueryState represents the lifecycle status of a SQL query.
type QueryState string

const (
	StatePending   QueryState = "PENDING"
	StateRunning   QueryState = "RUNNING"
	StateSucceeded QueryState = "SUCCEEDED"
	StateFailed    QueryState = "FAILED"
	StateCanceled  QueryState = "CANCELED"
	StateExpired   QueryState = "EXPIRED"
)

// IsTerminal returns true if the state cannot transition to any other active state.
func (s QueryState) IsTerminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCanceled, StateExpired:
		return true
	default:
		return false
	}
}

// CanTransitionTo checks if a state transition is valid based on the state machine rules.
func (s QueryState) CanTransitionTo(next QueryState) bool {
	if s == next {
		return true
	}
	switch s {
	case StatePending:
		return next == StateRunning || next == StateCanceled || next == StateFailed
	case StateRunning:
		return next == StateSucceeded || next == StateFailed || next == StateCanceled
	case StateSucceeded, StateFailed, StateCanceled:
		return next == StateExpired
	case StateExpired:
		return false
	default:
		return false
	}
}

// QueryOptions defines configuration overrides for a specific query execution.
type QueryOptions struct {
	Timeout        time.Duration `json:"timeout"`         // 0 = unlimited
	Mode           string        `json:"mode"`            // "sync" or "async"
	ResultTTL      time.Duration `json:"result_ttl"`      // how long to keep query results
	IdempotencyKey string        `json:"idempotency_key"` // uniqueness key
	ResultFormat   string        `json:"result_format"`   // "jsonl" (default), "csv", "parquet"
	StorageBackend string        `json:"storage_backend"` // override default storage engine
}

// QueryError encapsulates database or proxy-level query execution failures.
type QueryError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *QueryError) Error() string {
	return fmt.Sprintf("[%s] %s (retryable: %t)", e.Code, e.Message, e.Retryable)
}

// QueryStats tracks metrics regarding query execution and results materialization.
type QueryStats struct {
	RowsRead             int64         `json:"rows_read"`
	BytesWritten         int64         `json:"bytes_written"`
	DBExecDuration       time.Duration `json:"db_exec_duration"`
	StorageWriteDuration time.Duration `json:"storage_write_duration"`
	TotalDuration        time.Duration `json:"total_duration"`
	Retries              int32         `json:"retries"`
}

// ResultRef holds metadata about the serialized query results file.
type ResultRef struct {
	Backend   string `json:"backend"` // "fs", "s3", "clickhouse"
	Locator   string `json:"locator"` // path, bucket-key, or table name
	SizeBytes int64  `json:"size_bytes"`
	RowCount  int64  `json:"row_count"`
	Format    string `json:"format"`
	Checksum  string `json:"checksum"`
}

// QueryRecord contains the complete persistent state of a query execution request.
type QueryRecord struct {
	ID              string       `json:"id"`
	DatabaseID      string       `json:"database_id"`
	SQL             string       `json:"sql"`
	Options         QueryOptions `json:"options"`
	State           QueryState   `json:"state"`
	OwnerInstanceID string       `json:"owner_instance_id"`
	CreatedAt       time.Time    `json:"created_at"`
	StartedAt       time.Time    `json:"started_at,omitzero"`
	FinishedAt      time.Time    `json:"finished_at,omitzero"`
	Error           *QueryError  `json:"error,omitzero"`
	Stats           QueryStats   `json:"stats"`
	Result          *ResultRef   `json:"result,omitzero"`
	IdempotencyKey  string       `json:"idempotency_key,omitempty"`
	LeaseDeadline   time.Time    `json:"lease_deadline,omitzero"`
}

// DatabaseInfo represents static configuration and status metadata of a target database.
type DatabaseInfo struct {
	ID          string `json:"id"`
	Engine      string `json:"engine"` // oracle, postgres, mysql, clickhouse
	DisplayName string `json:"display_name"`
	Healthy     bool   `json:"healthy"`
}
