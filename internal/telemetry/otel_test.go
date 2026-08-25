package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// InitOTel with an empty endpoint must export nothing and return a working
// shutdown. The propagator is the exception: it is installed either way, so a
// deployment that exports through a collector it configures elsewhere still
// joins the caller's trace rather than starting a fresh one per request.
func TestInitOTel_NoEndpointIsNoOp(t *testing.T) {
	// A composite with nothing in it, so whatever extracts below is InitOTel's
	// doing rather than a leftover from another test.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	shutdown, err := InitOTel(context.Background(), "dbbridge-test", "")
	if err != nil {
		t.Fatalf("InitOTel: %v", err)
	}
	if shutdown == nil {
		t.Fatal("nil shutdown function")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}

	header := http.Header{}
	header.Set("traceparent", "00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01")
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(header))
	if got := trace.SpanContextFromContext(ctx).SpanID().String(); got != "1112131415161718" {
		t.Errorf("the global propagator extracted span id %q from a traceparent header, want 1112131415161718", got)
	}
}

// Handler must serve Prometheus metrics text.
func TestHandler_ServesMetrics(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	out := string(body)

	if !strings.Contains(out, "go_goroutines") {
		t.Error("expected baseline Go runtime metrics (go_goroutines) in output")
	}
	// go_sched_latencies_seconds is only exposed by a GoCollector backed by the
	// full runtime/metrics ruleset, not the legacy default collector (spec §11).
	if !strings.Contains(out, "go_sched_latencies_seconds") {
		t.Error("expected runtime/metrics-backed metric (go_sched_latencies_seconds) in output")
	}
}

// TestDomainMetricsReachPrometheus pins the fix for the split metrics stack:
// the domain instruments are OTel now, and Prometheus is one of the readers
// behind them, so /metrics must still carry them.
func TestDomainMetricsReachPrometheus(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	RecordQueryStarted()
	RecordQueryCompleted("postgres", "SUCCEEDED", 250*time.Millisecond)
	RecordQueryFinished()
	RecordResultBytes("fs", 4096)
	RecordIdempotencyHit()
	RecordPoolStat("pg_main", 4, 3, 1)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	out := string(body)

	for _, want := range []string{
		"dbbridge_queries_total",
		"dbbridge_query_duration_seconds",
		"dbbridge_inflight_queries",
		"dbbridge_result_bytes_total",
		"dbbridge_idempotency_hits_total",
		"dbbridge_db_pool_stats",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}
	if !strings.Contains(out, `engine="postgres"`) {
		t.Error("/metrics lost the engine label")
	}
	if !strings.Contains(out, `db_id="pg_main"`) {
		t.Error("/metrics lost the db_id label")
	}
}

// TestEnsureMeterProvider_RejectsLateConfiguration: Handler() builds the
// provider lazily, so serving /metrics before InitOTel used to leave OTLP
// exporting nothing for the life of the process, with target_info missing its
// service.name and not a line of diagnostic anywhere.
func TestEnsureMeterProvider_RejectsLateConfiguration(t *testing.T) {
	if _, err := ensureMeterProvider(nil, ""); err != nil {
		t.Fatalf("building the provider without an endpoint: %v", err)
	}
	if _, err := ensureMeterProvider(nil, "localhost:4317"); err == nil {
		t.Error("an OTLP endpoint arriving after the provider was built was accepted silently")
	}
}
