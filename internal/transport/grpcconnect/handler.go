package grpcconnect

import (
	"context"
	"errors"
	"io"
	"time"

	"dbbridge/internal/core/domain"
	"dbbridge/internal/core/service"
	v1 "dbbridge/internal/gen/api/proto/dbbridge/v1"
	"dbbridge/internal/gen/api/proto/dbbridge/v1/dbbridgev1connect"

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
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1.StartQueryResponse{
		Record: mapToProtoRecord(record),
	}), nil
}

func (h *QueryHandler) GetQueryStatus(ctx context.Context, req *connect.Request[v1.GetQueryStatusRequest]) (*connect.Response[v1.GetQueryStatusResponse], error) {
	record, err := h.svc.GetQueryStatus(ctx, req.Msg.QueryId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	return connect.NewResponse(&v1.GetQueryStatusResponse{
		Record: mapToProtoRecord(record),
	}), nil
}

func (h *QueryHandler) StopQuery(ctx context.Context, req *connect.Request[v1.StopQueryRequest]) (*connect.Response[v1.StopQueryResponse], error) {
	err := h.svc.StopQuery(ctx, req.Msg.QueryId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1.StopQueryResponse{
		QueryId: req.Msg.QueryId,
		Status:  "STOPPED",
	}), nil
}

func (h *QueryHandler) GetQueryStats(ctx context.Context, req *connect.Request[v1.GetQueryStatsRequest]) (*connect.Response[v1.GetQueryStatsResponse], error) {
	stats, err := h.svc.GetQueryStats(ctx, req.Msg.QueryId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	return connect.NewResponse(&v1.GetQueryStatsResponse{
		Stats: mapToProtoStats(stats),
	}), nil
}

func (h *QueryHandler) DownloadResult(ctx context.Context, req *connect.Request[v1.DownloadResultRequest], stream *connect.ServerStream[v1.DownloadResultResponse]) error {
	reader, _, err := h.svc.DownloadResult(ctx, req.Msg.QueryId, req.Msg.OffsetBytes, req.Msg.LimitBytes)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer reader.Close()

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
			return connect.NewError(connect.CodeInternal, err)
		}
	}

	return nil
}

func (h *QueryHandler) ListDatabases(ctx context.Context, req *connect.Request[v1.ListDatabasesRequest]) (*connect.Response[v1.ListDatabasesResponse], error) {
	dbs, err := h.svc.ListDatabases(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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
	err := h.svc.ReloadConfig(ctx)
	if err != nil {
		return connect.NewResponse(&v1.ReloadConfigResponse{
			Success: false,
			Message: err.Error(),
		}), nil
	}

	return connect.NewResponse(&v1.ReloadConfigResponse{
		Success: true,
		Message: "Config reloaded successfully",
	}), nil
}

func (h *QueryHandler) CanIBeStopped(ctx context.Context, req *connect.Request[v1.CanIBeStoppedRequest]) (*connect.Response[v1.CanIBeStoppedResponse], error) {
	canStop, inFlight := h.svc.CanIBeStopped(ctx)
	return connect.NewResponse(&v1.CanIBeStoppedResponse{
		CanBeStopped:  canStop,
		InFlightCount: int32(inFlight),
	}), nil
}

func (h *QueryHandler) WatchQuery(ctx context.Context, req *connect.Request[v1.WatchQueryRequest], stream *connect.ServerStream[v1.WatchQueryResponse]) error {
	eventCh, err := h.svc.WatchQuery(ctx, req.Msg.QueryId)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
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
					Code:      ev.Error.Code,
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

// Mappers

func mapToProtoRecord(r *domain.QueryRecord) *v1.QueryRecord {
	if r == nil {
		return nil
	}

	var protoErr *v1.QueryError
	if r.Error != nil {
		protoErr = &v1.QueryError{
			Code:      r.Error.Code,
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
		CreatedAtMs:     r.CreatedAt.UnixNano() / 1e6,
		StartedAtMs:     r.StartedAt.UnixNano() / 1e6,
		FinishedAtMs:    r.FinishedAt.UnixNano() / 1e6,
		Error:           protoErr,
		Stats:           mapToProtoStats(r.Stats),
		Result:          protoResult,
		IdempotencyKey:  r.IdempotencyKey,
		LeaseDeadlineMs: r.LeaseDeadline.UnixNano() / 1e6,
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
