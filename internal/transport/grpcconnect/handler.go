package grpcconnect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/core/service"
	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"
	"github.com/ekalinin/dbbridge/internal/ratelimit"
	"github.com/ekalinin/dbbridge/internal/state"

	"connectrpc.com/connect"
)

type QueryHandler struct {
	svc *service.QueryService
}

func NewQueryHandler(svc *service.QueryService) *QueryHandler {
	return &QueryHandler{svc: svc}
}

// Ensure interface compliance
var _ dbbridgev1connect.QueryServiceHandler = (*QueryHandler)(nil)

func (h *QueryHandler) StartQuery(ctx context.Context, req *connect.Request[v1.StartQueryRequest]) (*connect.Response[v1.StartQueryResponse], error) {
	msg := req.Msg
	opts := domain.QueryOptions{}
	if msg.Options != nil {
		opts = domain.QueryOptions{
			Timeout:        time.Duration(msg.Options.TimeoutMs) * time.Millisecond,
			Mode:           msg.Options.Mode,
			ResultTTL:      time.Duration(msg.Options.ResultTtlSeconds) * time.Second,
			IdempotencyKey: msg.Options.IdempotencyKey,
			ResultFormat:   msg.Options.ResultFormat,
			StorageBackend: msg.Options.StorageBackend,
		}
	}

	record, err := h.svc.StartQuery(ctx, msg.DatabaseId, msg.Sql, opts)
	if err != nil {
		return nil, connectError(err)
	}

	return connect.NewResponse(&v1.StartQueryResponse{
		Record: mapToProtoRecord(record),
	}), nil
}

func (h *QueryHandler) GetQueryStatus(ctx context.Context, req *connect.Request[v1.GetQueryStatusRequest]) (*connect.Response[v1.GetQueryStatusResponse], error) {
	record, err := h.svc.GetQueryStatus(ctx, req.Msg.QueryId)
	if err != nil {
		return nil, connectError(err)
	}

	return connect.NewResponse(&v1.GetQueryStatusResponse{
		Record: mapToProtoRecord(record),
	}), nil
}

func (h *QueryHandler) StopQuery(ctx context.Context, req *connect.Request[v1.StopQueryRequest]) (*connect.Response[v1.StopQueryResponse], error) {
	err := h.svc.StopQuery(ctx, req.Msg.QueryId)
	if err != nil {
		return nil, connectError(err)
	}

	return connect.NewResponse(&v1.StopQueryResponse{
		QueryId: req.Msg.QueryId,
		Status:  "STOPPED",
	}), nil
}

func (h *QueryHandler) GetQueryStats(ctx context.Context, req *connect.Request[v1.GetQueryStatsRequest]) (*connect.Response[v1.GetQueryStatsResponse], error) {
	stats, err := h.svc.GetQueryStats(ctx, req.Msg.QueryId)
	if err != nil {
		return nil, connectError(err)
	}

	return connect.NewResponse(&v1.GetQueryStatsResponse{
		Stats: mapToProtoStats(stats),
	}), nil
}

func (h *QueryHandler) DownloadResult(ctx context.Context, req *connect.Request[v1.DownloadResultRequest], stream *connect.ServerStream[v1.DownloadResultResponse]) error {
	reader, _, err := h.svc.DownloadResult(ctx, req.Msg.QueryId, req.Msg.OffsetBytes, req.Msg.LimitBytes)
	if err != nil {
		return connectError(err)
	}
	defer func() {
		if cerr := reader.Close(); cerr != nil {
			log.Printf("ERROR: failed to close result reader: %v", cerr)
		}
	}()

	// Stream in 64KB chunks
	buf := make([]byte, 64*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			sendErr := stream.Send(&v1.DownloadResultResponse{
				Chunk: buf[:n],
			})
			if sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return connectError(err)
		}
	}

	return nil
}

func (h *QueryHandler) ListDatabases(ctx context.Context, req *connect.Request[v1.ListDatabasesRequest]) (*connect.Response[v1.ListDatabasesResponse], error) {
	dbs, err := h.svc.ListDatabases(ctx)
	if err != nil {
		return nil, connectError(err)
	}

	protoDbs := make([]*v1.DatabaseInfo, len(dbs))
	for i, db := range dbs {
		protoDbs[i] = &v1.DatabaseInfo{
			Id:          db.ID,
			Engine:      db.Engine,
			DisplayName: db.DisplayName,
			Healthy:     db.Healthy,
		}
	}

	return connect.NewResponse(&v1.ListDatabasesResponse{
		Databases: protoDbs,
	}), nil
}

func (h *QueryHandler) ReloadConfig(ctx context.Context, req *connect.Request[v1.ReloadConfigRequest]) (*connect.Response[v1.ReloadConfigResponse], error) {
	report, err := h.svc.ReloadConfig(ctx)
	if err != nil {
		// A failed database pool carries the DSN (host, port, user); it belongs
		// in the log, never in the response - same rule as connectError below.
		log.Printf("ERROR: config reload failed: %v", err)
		return connect.NewResponse(&v1.ReloadConfigResponse{
			Success: false,
			Message: "config reload failed",
		}), nil
	}

	return connect.NewResponse(&v1.ReloadConfigResponse{
		Success: true,
		Message: fmt.Sprintf("Config reloaded successfully (added=%d removed=%d updated=%d)",
			len(report.Added), len(report.Removed), len(report.Updated)),
	}), nil
}

func (h *QueryHandler) CanIBeStopped(ctx context.Context, req *connect.Request[v1.CanIBeStoppedRequest]) (*connect.Response[v1.CanIBeStoppedResponse], error) {
	canStop, inFlight, st := h.svc.CanIBeStopped(ctx)
	return connect.NewResponse(&v1.CanIBeStoppedResponse{
		CanBeStopped:  canStop,
		InFlightCount: int32(inFlight),
		InstanceState: string(st),
	}), nil
}

func (h *QueryHandler) WatchQuery(ctx context.Context, req *connect.Request[v1.WatchQueryRequest], stream *connect.ServerStream[v1.WatchQueryResponse]) error {
	eventCh, err := h.svc.WatchQuery(ctx, req.Msg.QueryId)
	if err != nil {
		return connectError(err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-eventCh:
			if !ok {
				return nil
			}
			var protoErr *v1.QueryError
			if ev.Error != nil {
				protoErr = &v1.QueryError{
					Code:      string(ev.Error.Code),
					Message:   ev.Error.Message,
					Retryable: ev.Error.Retryable,
				}
			}

			sendErr := stream.Send(&v1.WatchQueryResponse{
				QueryId: ev.QueryID,
				State:   mapToProtoState(ev.State),
				Stats:   mapToProtoStats(ev.Stats),
				Error:   protoErr,
			})
			if sendErr != nil {
				return sendErr
			}
		}
	}
}

// connectError maps a domain error to a Connect code and a message that carries
// no internal detail. Everything except draining used to be CodeInternal with
// the wrapped error attached, and a driver connection failure spells out the
// host, the user and the connection parameters.
func connectError(err error) error {
	switch {
	case isType[domain.ValidationError](err):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case isType[domain.NotFoundError](err), errors.Is(err, state.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, errors.New("not found"))
	case isType[domain.DrainingError](err), isType[domain.UnavailableError](err):
		return connect.NewError(connect.CodeUnavailable, err)
	case isType[domain.ResourceExhaustedError](err):
		return connect.NewError(connect.CodeResourceExhausted, err)
	default:
		log.Printf("ERROR: rpc failed: %v", err)
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
}

func isType[T error](err error) bool {
	_, ok := errors.AsType[T](err)
	return ok
}

// unixMillis maps an unset timestamp to zero. time.Time{}.UnixNano()/1e6 gives
// a large negative number, so a PENDING record reported started_at_ms and
// finished_at_ms somewhere in the year 1754.
func unixMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// Mappers

func mapToProtoRecord(r *domain.QueryRecord) *v1.QueryRecord {
	if r == nil {
		return nil
	}

	var protoErr *v1.QueryError
	if r.Error != nil {
		protoErr = &v1.QueryError{
			Code:      string(r.Error.Code),
			Message:   r.Error.Message,
			Retryable: r.Error.Retryable,
		}
	}

	var protoResult *v1.ResultRef
	if r.Result != nil {
		protoResult = &v1.ResultRef{
			Backend:   r.Result.Backend,
			Locator:   r.Result.Locator,
			SizeBytes: r.Result.SizeBytes,
			RowCount:  r.Result.RowCount,
			Format:    r.Result.Format,
			Checksum:  r.Result.Checksum,
		}
	}

	return &v1.QueryRecord{
		Id:         r.ID,
		DatabaseId: r.DatabaseID,
		Sql:        r.SQL,
		Options: &v1.QueryOptions{
			TimeoutMs:        r.Options.Timeout.Milliseconds(),
			Mode:             r.Options.Mode,
			ResultTtlSeconds: int64(r.Options.ResultTTL.Seconds()),
			IdempotencyKey:   r.Options.IdempotencyKey,
			ResultFormat:     r.Options.ResultFormat,
			StorageBackend:   r.Options.StorageBackend,
		},
		State:           mapToProtoState(r.State),
		OwnerInstanceId: r.OwnerInstanceID,
		CreatedAtMs:     unixMillis(r.CreatedAt),
		StartedAtMs:     unixMillis(r.StartedAt),
		FinishedAtMs:    unixMillis(r.FinishedAt),
		Error:           protoErr,
		Stats:           mapToProtoStats(r.Stats),
		Result:          protoResult,
		IdempotencyKey:  r.IdempotencyKey,
		LeaseDeadlineMs: unixMillis(r.LeaseDeadline),
		Subject:         r.Subject,
	}
}

func mapToProtoState(s domain.QueryState) v1.QueryState {
	switch s {
	case domain.StatePending:
		return v1.QueryState_QUERY_STATE_PENDING
	case domain.StateRunning:
		return v1.QueryState_QUERY_STATE_RUNNING
	case domain.StateSucceeded:
		return v1.QueryState_QUERY_STATE_SUCCEEDED
	case domain.StateFailed:
		return v1.QueryState_QUERY_STATE_FAILED
	case domain.StateCanceled:
		return v1.QueryState_QUERY_STATE_CANCELED
	case domain.StateExpired:
		return v1.QueryState_QUERY_STATE_EXPIRED
	default:
		return v1.QueryState_QUERY_STATE_UNSPECIFIED
	}
}

func mapToProtoStats(s domain.QueryStats) *v1.QueryStats {
	return &v1.QueryStats{
		RowsRead:               s.RowsRead,
		BytesWritten:           s.BytesWritten,
		DbExecDurationMs:       s.DBExecDuration.Milliseconds(),
		StorageWriteDurationMs: s.StorageWriteDuration.Milliseconds(),
		TotalDurationMs:        s.TotalDuration.Milliseconds(),
		Retries:                s.Retries,
	}
}

// RateLimitInterceptor caps how fast a single caller can issue RPCs. It is
// keyed by peer address rather than by subject: it runs before authentication,
// so that a caller with no valid credential still cannot hammer the endpoint.
func RateLimitInterceptor(limiter *ratelimit.Limiter) connect.Interceptor {
	return rateLimitInterceptor{limiter: limiter}
}

type rateLimitInterceptor struct {
	limiter *ratelimit.Limiter
}

func (i rateLimitInterceptor) allow(peer connect.Peer) error {
	if i.limiter.Allow("addr:" + peer.Addr) {
		return nil
	}
	return connect.NewError(connect.CodeResourceExhausted, errors.New("rate limit exceeded"))
}

func (i rateLimitInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if req.Spec().IsClient {
			return next(ctx, req)
		}
		if err := i.allow(req.Peer()); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (i rateLimitInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i rateLimitInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := i.allow(conn.Peer()); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}
