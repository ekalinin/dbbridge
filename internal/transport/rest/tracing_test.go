package rest_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/rest"

	"github.com/coder/websocket"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// startQuery submits a sync query through the REST transport with the caller's
// trace context on it, and returns the remote SpanContext the caller published.
func startQuery(t *testing.T, url string) trace.SpanContext {
	t.Helper()

	body := `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync"}}`
	req, err := http.NewRequest(http.MethodPost, url+"/v1/queries", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	caller := testutil.InjectTraceContext(req.Header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/queries: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a sync query", resp.StatusCode)
	}
	return caller
}

// TestTracing_QuerySpanTree walks the whole chain spec §11 promises: the
// transport span adopts the caller's context, the service and the execution
// hang off it, and the database and storage phases are separate spans under the
// execution rather than one opaque query.run.
func TestTracing_QuerySpanTree(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	caller := startQuery(t, ts.URL)

	// A sync submission returns once the query is terminal, so every span below
	// query.run has ended by the time the response arrives.
	transport := testutil.SpanByName(t, recorder, "POST /v1/queries")
	if transport.Parent().SpanID() != caller.SpanID() {
		t.Errorf("transport span has parent %s, want the caller's %s",
			transport.Parent().SpanID(), caller.SpanID())
	}
	if transport.SpanContext().TraceID() != caller.TraceID() {
		t.Errorf("transport span is in trace %s, want the caller's %s",
			transport.SpanContext().TraceID(), caller.TraceID())
	}

	start := testutil.SpanByName(t, recorder, "StartQuery")
	testutil.RequireChildOf(t, start, transport)

	run := testutil.SpanByName(t, recorder, "query.run")
	testutil.RequireChildOf(t, testutil.SpanByName(t, recorder, "db.exec"), run)
	testutil.RequireChildOf(t, testutil.SpanByName(t, recorder, "db.rows"), run)
	testutil.RequireChildOf(t, testutil.SpanByName(t, recorder, "storage.write"), run)
}

// TestTracing_ExecutionLinksTheSubmitterRatherThanParenting covers the I1 seam:
// the execution outlives the request that submitted it, so it carries a link to
// the submitting span and must not be parented from it.
func TestTracing_ExecutionLinksTheSubmitterRatherThanParenting(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	startQuery(t, ts.URL)

	start := testutil.SpanByName(t, recorder, "StartQuery")
	run := testutil.SpanByName(t, recorder, "query.run")

	if run.Parent().IsValid() {
		t.Errorf("query.run has parent %s, want a root span: the execution does not "+
			"share a lifetime with the request that submitted it (I1)", run.Parent().SpanID())
	}

	var linked bool
	for _, link := range run.Links() {
		if link.SpanContext.SpanID() == start.SpanContext().SpanID() {
			linked = true
		}
	}
	if !linked {
		t.Errorf("query.run carries links %v, want one to the submitting span %s",
			run.Links(), start.SpanContext().SpanID())
	}
}

// TestTracing_DownloadIsTracedToTheBackend: the download used to be the least
// visible path of all - no transport span, no service span, and the read from
// the result store folded into whatever the caller was doing.
func TestTracing_DownloadIsTracedToTheBackend(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	startQuery(t, ts.URL)
	rec := testutil.SpanByName(t, recorder, "query.run")
	var queryID string
	for _, attr := range rec.Attributes() {
		if attr.Key == "query.id" {
			queryID = attr.Value.AsString()
		}
	}
	if queryID == "" {
		t.Fatal("query.run carries no query.id attribute")
	}

	resp, err := http.Get(ts.URL + "/v1/queries/" + queryID + "/result")
	if err != nil {
		t.Fatalf("GET result: %v", err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read result: %v", err)
	}
	resp.Body.Close()

	transport := testutil.SpanByName(t, recorder, "GET /v1/queries/{id}/result")
	download := testutil.SpanByName(t, recorder, "DownloadResult")
	testutil.RequireChildOf(t, download, transport)
	testutil.RequireChildOf(t, testutil.SpanByName(t, recorder, "storage.read"), download)
}

// TestTracing_WebSocketConnectionIsTraced: the WebSocket route is served by the
// same router, so a watch is a request like any other and shows up as one.
func TestTracing_WebSocketConnectionIsTraced(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	conn, _, err := websocket.Dial(context.Background(), ts.URL+"/v1/ws", nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
		t.Fatalf("close ws: %v", err)
	}

	// The span ends with the connection, so wait for the server to notice.
	waitForSpan(t, recorder, "GET /v1/ws")
}

func waitForSpan(t *testing.T, recorder *tracetest.SpanRecorder, name string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, span := range recorder.Ended() {
			if span.Name() == name {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no span named %q within the deadline, recorded: %v", name, testutil.SpanNames(recorder))
}

// TestTracing_ProbesProduceNoSpans: the kubelet and Prometheus call these on a
// fixed schedule, so a span apiece would be the bulk of the trace volume and
// none of its content.
func TestTracing_ProbesProduceNoSpans(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
	}

	if names := testutil.SpanNames(recorder); len(names) != 0 {
		t.Errorf("the probes produced spans %v, want none", names)
	}
}

// TestTracing_SpanIsNamedForTheRouteNotThePath: naming the span from the raw
// path would give every query ID a span name of its own, which is a name that
// groups nothing.
func TestTracing_SpanIsNamedForTheRouteNotThePath(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/v1/queries/no-such-query")
	if err != nil {
		t.Fatalf("GET /v1/queries/{id}: %v", err)
	}
	resp.Body.Close()

	span := testutil.SpanByName(t, recorder, "GET /v1/queries/{id}")
	if !hasRouteAttribute(span, "/v1/queries/{id}") {
		t.Errorf("span attributes = %v, want http.route=/v1/queries/{id}", span.Attributes())
	}
}

func hasRouteAttribute(span sdktrace.ReadOnlySpan, want string) bool {
	for _, attr := range span.Attributes() {
		if attr.Key == "http.route" && attr.Value.AsString() == want {
			return true
		}
	}
	return false
}
