package grpcconnect_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"
	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/grpcconnect"

	"connectrpc.com/connect"
)

// newTracedClient wires the handler with the tracing interceptor the way
// main.go does, and returns a client that publishes its own trace context on
// every call.
func newTracedClient(t *testing.T) dbbridgev1connect.QueryServiceClient {
	t.Helper()

	svc, _ := testutil.NewService(t)
	interceptor, err := grpcconnect.TracingInterceptor()
	if err != nil {
		t.Fatalf("TracingInterceptor: %v", err)
	}
	path, handler := dbbridgev1connect.NewQueryServiceHandler(
		grpcconnect.NewQueryHandler(svc), connect.WithInterceptors(interceptor))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return dbbridgev1connect.NewQueryServiceClient(srv.Client(), srv.URL)
}

// TestTracing_ConnectCallProducesATransportSpan: the Connect transport used to
// produce nothing, so StartQuery came out as a root span with the caller's
// trace nowhere in sight.
func TestTracing_ConnectCallProducesATransportSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	client := newTracedClient(t)

	req := connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "testdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "sync", ResultFormat: "jsonl"},
	})
	caller := testutil.InjectTraceContext(req.Header())

	if _, err := client.StartQuery(context.Background(), req); err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	// The client interceptor is not installed, so the only span under this name
	// is the handler's.
	transport := testutil.SpanByName(t, recorder, "dbbridge.v1.QueryService/StartQuery")
	if transport.Parent().SpanID() != caller.SpanID() {
		t.Errorf("transport span has parent %s, want the caller's %s",
			transport.Parent().SpanID(), caller.SpanID())
	}
	if transport.SpanContext().TraceID() != caller.TraceID() {
		t.Errorf("transport span is in trace %s, want the caller's %s",
			transport.SpanContext().TraceID(), caller.TraceID())
	}

	testutil.RequireChildOf(t, testutil.SpanByName(t, recorder, "StartQuery"), transport)
}

// TestTracing_ConnectReadPathsAreTraced covers the service spans behind the
// read RPCs: they produced nothing at all, so a status poll waiting on the
// MetaStore was invisible.
func TestTracing_ConnectReadPathsAreTraced(t *testing.T) {
	// Before the client: the interceptor resolves its tracer from the global
	// provider once, when it is built.
	recorder := testutil.RecordSpans(t)
	client := newTracedClient(t)

	started, err := client.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "testdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "sync", ResultFormat: "jsonl"},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	queryID := started.Msg.Record.Id

	if _, err := client.GetQueryStatus(context.Background(), connect.NewRequest(
		&v1.GetQueryStatusRequest{QueryId: queryID})); err != nil {
		t.Fatalf("GetQueryStatus: %v", err)
	}
	if _, err := client.GetQueryStats(context.Background(), connect.NewRequest(
		&v1.GetQueryStatsRequest{QueryId: queryID})); err != nil {
		t.Fatalf("GetQueryStats: %v", err)
	}
	if _, err := client.ListDatabases(context.Background(), connect.NewRequest(
		&v1.ListDatabasesRequest{})); err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}

	for _, name := range []string{"GetQueryStatus", "GetQueryStats", "ListDatabases"} {
		span := testutil.SpanByName(t, recorder, name)
		testutil.RequireChildOf(t, span,
			testutil.SpanByName(t, recorder, "dbbridge.v1.QueryService/"+name))
	}
}
