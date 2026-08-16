package grpcconnect_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/core/service"
	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"
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

// TestConnect_ZeroTimestamps: time.Time{}.UnixNano()/1e6 is a large negative
// number, so a PENDING record used to report started_at_ms and finished_at_ms
// somewhere in the year 1754.
func TestConnect_ZeroTimestamps(t *testing.T) {
	svc, _ := testutil.NewService(t)
	h := grpcconnect.NewQueryHandler(svc)

	resp, err := h.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "slowdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "async"},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	rec := resp.Msg.Record
	if rec.StartedAtMs < 0 || rec.FinishedAtMs < 0 || rec.LeaseDeadlineMs < 0 {
		t.Errorf("unset timestamps are negative: started=%d finished=%d lease=%d",
			rec.StartedAtMs, rec.FinishedAtMs, rec.LeaseDeadlineMs)
	}
	if rec.CreatedAtMs <= 0 {
		t.Errorf("created_at_ms = %d, want a real timestamp", rec.CreatedAtMs)
	}
}

// TestConnect_ErrorCodes: every failure except draining used to be CodeInternal
// with the wrapped driver error, DSN included, attached to it.
func TestConnect_ErrorCodes(t *testing.T) {
	svc, _ := testutil.NewService(t)
	h := grpcconnect.NewQueryHandler(svc)

	cases := []struct {
		name string
		req  *v1.StartQueryRequest
		want connect.Code
	}{
		{"unknown database", &v1.StartQueryRequest{DatabaseId: "nope", Sql: "SELECT 1"}, connect.CodeNotFound},
		{"bad format", &v1.StartQueryRequest{DatabaseId: "testdb", Sql: "SELECT 1",
			Options: &v1.QueryOptions{ResultFormat: "parquet"}}, connect.CodeInvalidArgument},
		{"write statement", &v1.StartQueryRequest{DatabaseId: "testdb", Sql: "DROP TABLE t"}, connect.CodeInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.StartQuery(context.Background(), connect.NewRequest(tc.req))
			if err == nil {
				t.Fatal("StartQuery succeeded, want an error")
			}
			if got := connect.CodeOf(err); got != tc.want {
				t.Errorf("code = %v, want %v", got, tc.want)
			}
			lower := strings.ToLower(err.Error())
			for _, leak := range []string{"dsn", "password", "postgres://", "fake@"} {
				if strings.Contains(lower, leak) {
					t.Errorf("error leaks %q: %v", leak, err)
				}
			}
		})
	}

	if _, err := h.GetQueryStatus(context.Background(), connect.NewRequest(&v1.GetQueryStatusRequest{
		QueryId: "no-such-query",
	})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetQueryStatus code = %v, want NotFound", connect.CodeOf(err))
	}
}

// TestConnect_RequiresCredentials: there was no interceptor at all, so the gRPC
// surface ran arbitrary SQL for anyone who could reach the port. Streaming RPCs
// are covered too, they go through a different interceptor hook.
func TestConnect_RequiresCredentials(t *testing.T) {
	svc, _ := testutil.NewService(t)
	auth, err := authn.New([]authn.TokenSpec{
		{Subject: "alice", Value: "alice-token", Scopes: []string{"read", "write"}},
		{Subject: "watcher", Value: "read-token", Scopes: []string{"read"}},
		{Subject: "root", Value: "admin-token", Scopes: []string{"admin"}},
	})
	if err != nil {
		t.Fatalf("authn.New: %v", err)
	}

	mux := http.NewServeMux()
	path, handler := dbbridgev1connect.NewQueryServiceHandler(
		grpcconnect.NewQueryHandler(svc),
		connect.WithInterceptors(grpcconnect.NewAuthInterceptor(auth)),
	)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client := func(token string) dbbridgev1connect.QueryServiceClient {
		opts := []connect.ClientOption{}
		if token != "" {
			opts = append(opts, connect.WithInterceptors(
				connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
					return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
						req.Header().Set("Authorization", "Bearer "+token)
						return next(ctx, req)
					}
				}),
			))
		}
		return dbbridgev1connect.NewQueryServiceClient(ts.Client(), ts.URL, opts...)
	}

	start := &v1.StartQueryRequest{DatabaseId: "testdb", Sql: "SELECT 1"}

	if _, err := client("").StartQuery(context.Background(), connect.NewRequest(start)); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("without a token: code = %v, want Unauthenticated", connect.CodeOf(err))
	}
	if _, err := client("read-token").StartQuery(context.Background(), connect.NewRequest(start)); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("with a read-only token: code = %v, want PermissionDenied", connect.CodeOf(err))
	}
	if _, err := client("alice-token").StartQuery(context.Background(), connect.NewRequest(start)); err != nil {
		t.Errorf("with a write token: %v", err)
	}
	if _, err := client("alice-token").ReloadConfig(context.Background(), connect.NewRequest(&v1.ReloadConfigRequest{})); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("reload with a write token: code = %v, want PermissionDenied", connect.CodeOf(err))
	}

	// A server stream must not be reachable without a credential either.
	stream, err := client("").WatchQuery(context.Background(), connect.NewRequest(&v1.WatchQueryRequest{QueryId: "x"}))
	if err == nil {
		stream.Receive()
		err = stream.Err()
		if cerr := stream.Close(); cerr != nil {
			t.Logf("close stream: %v", cerr)
		}
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("streaming without a token: code = %v, want Unauthenticated", connect.CodeOf(err))
	}
}

// TestConnect_QueriesAreBoundToTheirSubject: the scopes were tested on this
// surface but the subject binding was not, and that is the second of the two
// properties the credential work is for.
func TestConnect_QueriesAreBoundToTheirSubject(t *testing.T) {
	svc, _ := testutil.NewService(t)
	auth, err := authn.New([]authn.TokenSpec{
		{Subject: "alice", Value: "alice-token", Scopes: []string{"read", "write"}},
		{Subject: "bob", Value: "bob-token", Scopes: []string{"read", "write"}},
		{Subject: "root", Value: "admin-token", Scopes: []string{"admin"}},
	})
	if err != nil {
		t.Fatalf("authn.New: %v", err)
	}

	mux := http.NewServeMux()
	path, handler := dbbridgev1connect.NewQueryServiceHandler(
		grpcconnect.NewQueryHandler(svc),
		connect.WithInterceptors(grpcconnect.NewAuthInterceptor(auth)),
	)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client := func(token string) dbbridgev1connect.QueryServiceClient {
		return dbbridgev1connect.NewQueryServiceClient(ts.Client(), ts.URL, connect.WithInterceptors(
			connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
					req.Header().Set("Authorization", "Bearer "+token)
					return next(ctx, req)
				}
			}),
		))
	}

	started, err := client("alice-token").StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "testdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "sync"},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	id := started.Msg.GetRecord().GetId()
	if id == "" {
		t.Fatal("no query id in the response")
	}

	status := &v1.GetQueryStatusRequest{QueryId: id}
	if _, err := client("alice-token").GetQueryStatus(context.Background(), connect.NewRequest(status)); err != nil {
		t.Errorf("the owner could not read its own query: %v", err)
	}
	// A different subject must not learn that the ID exists.
	if _, err := client("bob-token").GetQueryStatus(context.Background(), connect.NewRequest(status)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("foreign GetQueryStatus: code = %v, want NotFound", connect.CodeOf(err))
	}
	if _, err := client("admin-token").GetQueryStatus(context.Background(), connect.NewRequest(status)); err != nil {
		t.Errorf("admin was denied: %v", err)
	}
	if _, err := client("bob-token").StopQuery(context.Background(), connect.NewRequest(&v1.StopQueryRequest{QueryId: id})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("foreign StopQuery: code = %v, want NotFound", connect.CodeOf(err))
	}
}
