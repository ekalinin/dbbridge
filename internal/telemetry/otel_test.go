package telemetry

import (
	"context"
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

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "go_") {
		t.Error("expected Go runtime metrics in output")
	}
}
