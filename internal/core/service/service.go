package service

import (
	"context"
	"fmt"
	"io"

	"dbbridge/internal/core/domain"
	"dbbridge/internal/core/manager"
	"dbbridge/internal/lifecycle"
	"dbbridge/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type QueryService struct {
	qm        *manager.QueryManager
	lifecycle *lifecycle.Manager
}

func NewQueryService(qm *manager.QueryManager, lm *lifecycle.Manager) *QueryService {
	return &QueryService{
		qm:        qm,
		lifecycle: lm,
	}
}

func (s *QueryService) StartQuery(ctx context.Context, dbID string, sql string, opts domain.QueryOptions) (*domain.QueryRecord, error) {
	ctx, span := otel.Tracer("dbbridge").Start(ctx, "StartQuery",
		trace.WithAttributes(
			attribute.String("query.database_id", dbID),
			attribute.String("query.mode", opts.Mode),
		))
	defer span.End()

	if s.lifecycle.IsDraining() {
		return nil, fmt.Errorf("service is draining: new queries are not accepted")
	}
	return s.qm.SubmitQuery(ctx, dbID, sql, opts)
}

func (s *QueryService) GetQueryStatus(ctx context.Context, queryID string) (*domain.QueryRecord, error) {
	return s.qm.GetQuery(ctx, queryID)
}

func (s *QueryService) StopQuery(ctx context.Context, queryID string) error {
	return s.qm.StopQuery(ctx, queryID)
}

func (s *QueryService) GetQueryStats(ctx context.Context, queryID string) (domain.QueryStats, error) {
	rec, err := s.qm.GetQuery(ctx, queryID)
	if err != nil {
		return domain.QueryStats{}, err
	}
	return rec.Stats, nil
}

func (s *QueryService) DownloadResult(ctx context.Context, queryID string, offset, limit int64) (io.ReadCloser, domain.ResultRef, error) {
	rec, err := s.qm.GetQuery(ctx, queryID)
	if err != nil {
		return nil, domain.ResultRef{}, err
	}

	if rec.State != domain.StateSucceeded {
		return nil, domain.ResultRef{}, fmt.Errorf("query %s is in state %s, cannot download results", queryID, rec.State)
	}

	if rec.Result == nil {
		return nil, domain.ResultRef{}, fmt.Errorf("no results associated with query %s", queryID)
	}

	store, err := storage.GetStore(rec.Result.Backend)
	if err != nil {
		return nil, domain.ResultRef{}, fmt.Errorf("failed to load result storage: %w", err)
	}

	reader, err := store.Reader(ctx, *rec.Result)
	if err != nil {
		return nil, domain.ResultRef{}, fmt.Errorf("failed to open result reader: %w", err)
	}

	// If offset and limit are specified, wrap with SectionReader or custom logic.
	// Since s3 and fs readers might not support Seek natively, we can wrap with a skip/limit reader.
	if offset > 0 || limit > 0 {
		return newSectionReadCloser(reader, offset, limit), *rec.Result, nil
	}

	return reader, *rec.Result, nil
}

func (s *QueryService) ListDatabases(ctx context.Context) ([]domain.DatabaseInfo, error) {
	// Extract databases from active configuration
	cfg := s.qm.GetConfig()
	databases := make([]domain.DatabaseInfo, 0, len(cfg.Databases))

	for _, dbCfg := range cfg.Databases {
		healthy := true
		// Verify health status by checking if we have pool
		pool, exists := s.qm.GetPool(dbCfg.ID)
		if !exists || pool.Ping(ctx) != nil {
			healthy = false
		}

		databases = append(databases, domain.DatabaseInfo{
			ID:          dbCfg.ID,
			Engine:      dbCfg.Engine,
			DisplayName: dbCfg.DisplayName,
			Healthy:     healthy,
		})
	}

	return databases, nil
}

func (s *QueryService) ReloadConfig(ctx context.Context) error {
	return s.qm.Reload()
}

func (s *QueryService) CanIBeStopped(ctx context.Context) (bool, int) {
	inFlight := s.qm.CountInFlight()
	return inFlight == 0, inFlight
}

func (s *QueryService) WatchQuery(ctx context.Context, queryID string) (<-chan manager.QueryEvent, error) {
	return s.qm.Watch(ctx, queryID)
}

// Helper methods to bind QueryManager to Service nicely

// We need to add GetConfig and GetPool to manager.go to support this.
// Let's implement sectionReadCloser.

type sectionReadCloser struct {
	r      io.ReadCloser
	offset int64
	limit  int64
	read   int64
}

func newSectionReadCloser(r io.ReadCloser, offset, limit int64) io.ReadCloser {
	return &sectionReadCloser{
		r:      r,
		offset: offset,
		limit:  limit,
	}
}

func (src *sectionReadCloser) Read(p []byte) (int, error) {
	// First, skip the offset bytes
	if src.offset > 0 {
		discarded, err := io.CopyN(io.Discard, src.r, src.offset)
		src.offset -= discarded
		if err != nil {
			return 0, err
		}
	}

	if src.limit > 0 && src.read >= src.limit {
		return 0, io.EOF
	}

	toRead := p
	if src.limit > 0 {
		remaining := src.limit - src.read
		if int64(len(p)) > remaining {
			toRead = p[:remaining]
		}
	}

	n, err := src.r.Read(toRead)
	src.read += int64(n)

	if src.limit > 0 && src.read >= src.limit && err == nil {
		return n, io.EOF
	}

	return n, err
}

func (src *sectionReadCloser) Close() error {
	return src.r.Close()
}
