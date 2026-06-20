package grpcconnect_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/service"
	v1 "github.com/ekalinin/dbbridge/internal/gen/api/proto/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/gen/api/proto/dbbridge/v1/dbbridgev1connect"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/grpcconnect"

	"connectrpc.com/connect"
)

func newClient(t *testing.T) (dbbridgev1connect.QueryServiceClient, *service.QueryService) {
	t.Helper()
	svc, _ := testutil.NewService(t)
	h := grpcconnect.NewQueryHandler(svc)
	mux := http.NewServeMux()
	path, handler := dbbridgev1connect.NewQueryServiceHandler(h)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return dbbridgev1connect.NewQueryServiceClient(srv.Client(), srv.URL), svc
}

func startSync(t *testing.T, c dbbridgev1connect.QueryServiceClient, dbID, sql string) *v1.QueryRecord {
	t.Helper()
	resp, err := c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: dbID,
		Sql:        sql,
		Options:    &v1.QueryOptions{Mode: "sync", ResultFormat: "jsonl"},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	return resp.Msg.Record
}

func TestConnect_StartQuerySync(t *testing.T) {
	c, _ := newClient(t)
	rec := startSync(t, c, "testdb", "SELECT 1")
	if rec.State != v1.QueryState_QUERY_STATE_SUCCEEDED {
		t.Fatalf("state = %v, want SUCCEEDED", rec.State)
	}
	if rec.Id == "" {
		t.Fatal("empty id")
	}
}

func TestConnect_GetStatusAndStats(t *testing.T) {
	c, _ := newClient(t)
	rec := startSync(t, c, "testdb", "SELECT 1")

	st, err := c.GetQueryStatus(context.Background(), connect.NewRequest(&v1.GetQueryStatusRequest{QueryId: rec.Id}))
	if err != nil {
		t.Fatalf("GetQueryStatus: %v", err)
	}
	if st.Msg.Record.State != v1.QueryState_QUERY_STATE_SUCCEEDED {
		t.Errorf("status state = %v", st.Msg.Record.State)
	}

	stats, err := c.GetQueryStats(context.Background(), connect.NewRequest(&v1.GetQueryStatsRequest{QueryId: rec.Id}))
	if err != nil {
		t.Fatalf("GetQueryStats: %v", err)
	}
	if stats.Msg.Stats.RowsRead != 2 {
		t.Errorf("rows_read = %d, want 2", stats.Msg.Stats.RowsRead)
	}
}

func TestConnect_GetStatus_NotFound(t *testing.T) {
	c, _ := newClient(t)
	_, err := c.GetQueryStatus(context.Background(), connect.NewRequest(&v1.GetQueryStatusRequest{QueryId: "nope"}))
	if err == nil {
		t.Fatal("expected error for unknown query")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", connect.CodeOf(err))
	}
}

func TestConnect_ListDatabases(t *testing.T) {
	c, _ := newClient(t)
	resp, err := c.ListDatabases(context.Background(), connect.NewRequest(&v1.ListDatabasesRequest{}))
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(resp.Msg.Databases) != 2 {
		t.Errorf("got %d databases, want 2", len(resp.Msg.Databases))
	}
}

func TestConnect_CanIBeStopped(t *testing.T) {
	c, _ := newClient(t)
	resp, err := c.CanIBeStopped(context.Background(), connect.NewRequest(&v1.CanIBeStoppedRequest{}))
	if err != nil {
		t.Fatalf("CanIBeStopped: %v", err)
	}
	if !resp.Msg.CanBeStopped || resp.Msg.InFlightCount != 0 {
		t.Errorf("can_stop=%v in_flight=%d", resp.Msg.CanBeStopped, resp.Msg.InFlightCount)
	}
}

func TestConnect_ReloadConfig(t *testing.T) {
	c, _ := newClient(t)
	resp, err := c.ReloadConfig(context.Background(), connect.NewRequest(&v1.ReloadConfigRequest{}))
	if err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}
	if !resp.Msg.Success {
		t.Errorf("reload success = false: %s", resp.Msg.Message)
	}
}

func TestConnect_StartQuery_DrainingUnavailable(t *testing.T) {
	svc, lm := testutil.NewService(t)
	h := grpcconnect.NewQueryHandler(svc)
	mux := http.NewServeMux()
	path, handler := dbbridgev1connect.NewQueryServiceHandler(h)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := dbbridgev1connect.NewQueryServiceClient(srv.Client(), srv.URL)

	lm.SetState(lifecycle.StateDraining)

	_, err := c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "testdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "sync"},
	}))
	if err == nil {
		t.Fatal("expected error while draining")
	}
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("code = %v, want Unavailable", connect.CodeOf(err))
	}
}

func TestConnect_DownloadResultStream(t *testing.T) {
	c, _ := newClient(t)
	rec := startSync(t, c, "testdb", "SELECT id, name FROM t")

	stream, err := c.DownloadResult(context.Background(), connect.NewRequest(&v1.DownloadResultRequest{QueryId: rec.Id}))
	if err != nil {
		t.Fatalf("DownloadResult: %v", err)
	}
	var buf bytes.Buffer
	for stream.Receive() {
		buf.Write(stream.Msg().Chunk)
	}
	if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("stream err: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("downloaded empty result")
	}
	if !bytes.Contains(buf.Bytes(), []byte("alice")) {
		t.Errorf("result missing expected data: %s", buf.String())
	}
}

func TestConnect_WatchQueryStream(t *testing.T) {
	c, svc := newClient(t)

	// Start a long-running query that stays RUNNING until canceled.
	start, err := c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "slowdb",
		Sql:        "SELECT *",
		Options:    &v1.QueryOptions{Mode: "async"},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	id := start.Msg.Record.Id

	// Stop the query shortly after the watch subscription is established. The
	// Connect server-stream client blocks until the first event (headers flush
	// on first Send), so the stop must be scheduled before the WatchQuery call.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = svc.StopQuery(context.Background(), id)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := c.WatchQuery(ctx, connect.NewRequest(&v1.WatchQueryRequest{QueryId: id}))
	if err != nil {
		t.Fatalf("WatchQuery: %v", err)
	}

	sawTerminal := false
	for stream.Receive() {
		st := stream.Msg().State
		if st == v1.QueryState_QUERY_STATE_CANCELED ||
			st == v1.QueryState_QUERY_STATE_FAILED ||
			st == v1.QueryState_QUERY_STATE_SUCCEEDED {
			sawTerminal = true
			break
		}
	}
	if !sawTerminal {
		t.Errorf("did not observe a terminal state via WatchQuery (stream err: %v)", stream.Err())
	}
}
