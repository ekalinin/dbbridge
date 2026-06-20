package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// InitOTel with an empty endpoint must be a no-op and return a working shutdown.
func TestInitOTel_NoEndpointIsNoOp(t *testing.T) {
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
	// full runtime/metrics ruleset, not the legacy default collector (spec §10).
	if !strings.Contains(out, "go_sched_latencies_seconds") {
		t.Error("expected runtime/metrics-backed metric (go_sched_latencies_seconds) in output")
	}
}
