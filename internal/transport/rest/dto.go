package rest

import (
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

// The wire types below exist because the domain model is not the REST contract.
// Serializing domain.QueryRecord directly emitted `timeout`, `result_ttl` and
// `db_exec_duration` in nanoseconds while api/openapi/dbbridge.yaml promised
// `timeout_ms`, `result_ttl_seconds` and `db_exec_duration_ms`; the API even
// accepted milliseconds on the way in and answered in nanoseconds on the way
// out, so no generated client could read a response correctly.

type queryOptionsDTO struct {
	TimeoutMs        int64  `json:"timeout_ms"`
	Mode             string `json:"mode"`
	ResultTTLSeconds int64  `json:"result_ttl_seconds"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	ResultFormat     string `json:"result_format"`
	StorageBackend   string `json:"storage_backend"`
}

type queryStatsDTO struct {
	RowsRead               int64 `json:"rows_read"`
	BytesWritten           int64 `json:"bytes_written"`
	DBExecDurationMs       int64 `json:"db_exec_duration_ms"`
	StorageWriteDurationMs int64 `json:"storage_write_duration_ms"`
	TotalDurationMs        int64 `json:"total_duration_ms"`
	Retries                int32 `json:"retries"`
}

type resultRefDTO struct {
	Backend   string `json:"backend"`
	Locator   string `json:"locator"`
	SizeBytes int64  `json:"size_bytes"`
	RowCount  int64  `json:"row_count"`
	Format    string `json:"format"`
	Checksum  string `json:"checksum,omitempty"`
}

type queryErrorDTO struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type queryRecordDTO struct {
	ID              string          `json:"id"`
	DatabaseID      string          `json:"database_id"`
	SQL             string          `json:"sql"`
	Options         queryOptionsDTO `json:"options"`
	State           string          `json:"state"`
	OwnerInstanceID string          `json:"owner_instance_id"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	LeaseDeadline   *time.Time      `json:"lease_deadline,omitempty"`
	Error           *queryErrorDTO  `json:"error,omitempty"`
	Stats           queryStatsDTO   `json:"stats"`
	Result          *resultRefDTO   `json:"result,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Subject         string          `json:"subject,omitempty"`
}

// optionalTime maps a zero time.Time to an omitted field instead of to the year
// one, which is what a client reads as "started in 0001-01-01".
func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func toOptionsDTO(o domain.QueryOptions) queryOptionsDTO {
	return queryOptionsDTO{
		TimeoutMs:        o.Timeout.Milliseconds(),
		Mode:             o.Mode,
		ResultTTLSeconds: int64(o.ResultTTL.Seconds()),
		IdempotencyKey:   o.IdempotencyKey,
		ResultFormat:     o.ResultFormat,
		StorageBackend:   o.StorageBackend,
	}
}

func toStatsDTO(s domain.QueryStats) queryStatsDTO {
	return queryStatsDTO{
		RowsRead:               s.RowsRead,
		BytesWritten:           s.BytesWritten,
		DBExecDurationMs:       s.DBExecDuration.Milliseconds(),
		StorageWriteDurationMs: s.StorageWriteDuration.Milliseconds(),
		TotalDurationMs:        s.TotalDuration.Milliseconds(),
		Retries:                s.Retries,
	}
}

func toRecordDTO(r *domain.QueryRecord) *queryRecordDTO {
	if r == nil {
		return nil
	}

	dto := &queryRecordDTO{
		ID:              r.ID,
		DatabaseID:      r.DatabaseID,
		SQL:             r.SQL,
		Options:         toOptionsDTO(r.Options),
		State:           string(r.State),
		OwnerInstanceID: r.OwnerInstanceID,
		CreatedAt:       r.CreatedAt,
		StartedAt:       optionalTime(r.StartedAt),
		FinishedAt:      optionalTime(r.FinishedAt),
		LeaseDeadline:   optionalTime(r.LeaseDeadline),
		Stats:           toStatsDTO(r.Stats),
		IdempotencyKey:  r.IdempotencyKey,
		Subject:         r.Subject,
	}

	if r.Error != nil {
		dto.Error = &queryErrorDTO{
			Code:      string(r.Error.Code),
			Message:   r.Error.Message,
			Retryable: r.Error.Retryable,
		}
	}
	if r.Result != nil {
		dto.Result = &resultRefDTO{
			Backend:   r.Result.Backend,
			Locator:   r.Result.Locator,
			SizeBytes: r.Result.SizeBytes,
			RowCount:  r.Result.RowCount,
			Format:    r.Result.Format,
			Checksum:  r.Result.Checksum,
		}
	}

	return dto
}
