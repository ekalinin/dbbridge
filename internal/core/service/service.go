package service

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type QueryService struct {
	qm        *manager.QueryManager
	lifecycle *lifecycle.Manager
	// authRequired records that the deployment runs with credentials, so a
	// request that arrives without an identity is a transport that forgot to
	// authenticate rather than a deployment that chose not to.
	authRequired bool
}

func NewQueryService(qm *manager.QueryManager, lm *lifecycle.Manager) *QueryService {
	return &QueryService{
		qm:        qm,
		lifecycle: lm,
	}
}

// SetAuthRequired tells the service that every caller is expected to carry an
// identity. It is called once at startup, before any request is served.
func (s *QueryService) SetAuthRequired(v bool) {
	s.authRequired = v
}

func (s *QueryService) StartQuery(ctx context.Context, dbID string, sql string, opts domain.QueryOptions) (*domain.QueryRecord, error) {
	ctx, span := otel.Tracer("dbbridge").Start(ctx, "StartQuery",
		trace.WithAttributes(
			attribute.String("query.database_id", dbID),
			attribute.String("query.mode", opts.Mode),
		))
	defer span.End()

	if s.lifecycle.IsDraining() {
		return nil, domain.DrainingError{}
	}
	return s.qm.SubmitQuery(ctx, dbID, sql, opts)
}

// authorized fetches a record and checks the caller may see it. Knowing a query
// ID used to be enough to read anyone's SQL, status, stats and result; a
// non-admin caller now only reaches its own queries. A record whose subject is
// unknown to the caller is reported as not found rather than as forbidden, so
// the API does not confirm that an ID exists.
func (s *QueryService) authorized(ctx context.Context, queryID string) (*domain.QueryRecord, error) {
	// AuthorizeSubject reads a missing identity as "authentication is disabled"
	// and lets the call through. That is right for a deployment without
	// credentials and wrong for one with them, where it would turn a route that
	// slipped past the gate into anonymous access to every subject's records.
	if s.authRequired {
		if _, ok := authn.FromContext(ctx); !ok {
			return nil, domain.NotFoundError{Resource: "query", ID: queryID}
		}
	}
	rec, err := s.qm.GetQuery(ctx, queryID)
	if err != nil {
		return nil, err
	}
	if err := authn.AuthorizeSubject(ctx, rec.Subject); err != nil {
		return nil, domain.NotFoundError{Resource: "query", ID: queryID}
	}
	return rec, nil
}

func (s *QueryService) GetQueryStatus(ctx context.Context, queryID string) (*domain.QueryRecord, error) {
	return s.authorized(ctx, queryID)
}

func (s *QueryService) StopQuery(ctx context.Context, queryID string) error {
	if _, err := s.authorized(ctx, queryID); err != nil {
		return err
	}
	return s.qm.StopQuery(ctx, queryID)
}

func (s *QueryService) GetQueryStats(ctx context.Context, queryID string) (domain.QueryStats, error) {
	rec, err := s.authorized(ctx, queryID)
	if err != nil {
		return domain.QueryStats{}, err
	}
	return rec.Stats, nil
}

func (s *QueryService) DownloadResult(ctx context.Context, queryID string, offset, limit int64) (io.ReadCloser, domain.ResultRef, error) {
	rec, err := s.authorized(ctx, queryID)
	if err != nil {
		return nil, domain.ResultRef{}, err
	}

	if rec.State != domain.StateSucceeded {
		return nil, domain.ResultRef{}, domain.ValidationError{
			Field:  "query_id",
			Reason: "query is in state " + string(rec.State) + ", results are only available for SUCCEEDED",
		}
	}

	if rec.Result == nil {
		return nil, domain.ResultRef{}, domain.NotFoundError{Resource: "result for query", ID: queryID}
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
	configured := make(map[string]struct{}, len(cfg.Databases))

	for _, dbCfg := range cfg.Databases {
		configured[dbCfg.ID] = struct{}{}
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

	// Databases that still hold retained results but were dropped from the
	// configuration are reported as unhealthy rather than hidden: their query
	// records and results are still downloadable (I2).
	seen, err := s.qm.DatabasesSeen(ctx)
	if err != nil {
		log.Printf("ERROR: failed to list databases seen: %v", err)
		return databases, nil
	}
	for _, id := range seen {
		if id == "" {
			continue
		}
		if _, ok := configured[id]; ok {
			continue
		}
		databases = append(databases, domain.DatabaseInfo{ID: id, Healthy: false})
	}

	return databases, nil
}

func (s *QueryService) ReloadConfig(ctx context.Context) (domain.ReloadReport, error) {
	return s.qm.Reload()
}

// CanIBeStopped reports whether the orchestrator may terminate this instance.
// The count comes from the MetaStore as well as the local registry, so a node
// restarted under the same instance ID does not claim to be quiesced while
// records it owns are still marked in-flight (I5, spec §9).
func (s *QueryService) CanIBeStopped(ctx context.Context) (bool, int, lifecycle.State) {
	inFlight := s.qm.CountInFlight(ctx)
	// A draining instance with nothing left in flight advances to STOPPABLE,
	// the third lifecycle state the spec defines (§9).
	st := s.lifecycle.Advance(inFlight)
	return inFlight == 0, inFlight, st
}

// InstanceState reports the lifecycle state of this instance.
func (s *QueryService) InstanceState() lifecycle.State {
	return s.lifecycle.GetState()
}

// IsDraining reports whether the instance is draining. The readiness probe uses
// it to take this node out of the load balancer rotation during graceful drain.
func (s *QueryService) IsDraining() bool {
	return s.lifecycle.IsDraining()
}

func (s *QueryService) WatchQuery(ctx context.Context, queryID string) (<-chan manager.QueryEvent, error) {
	if _, err := s.authorized(ctx, queryID); err != nil {
		return nil, err
	}
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
