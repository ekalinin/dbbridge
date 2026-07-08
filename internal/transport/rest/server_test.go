package rest_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/rest"
)

func TestREST_StartQuery_DrainingReturns503(t *testing.T) {
	svc, lm := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc).Handler())
	t.Cleanup(ts.Close)

	lm.SetState(lifecycle.StateDraining)

	body := `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync"}}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/queries: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 Service Unavailable", resp.StatusCode)
	}
}

func TestREST_Readyz_ReflectsDraining(t *testing.T) {
	svc, lm := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc).Handler())
	t.Cleanup(ts.Close)

	// Serving -> ready (200).
	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("while serving: status = %d, want 200", resp.StatusCode)
	}

	// Draining -> not ready (503), so the LB removes this node from rotation.
	lm.SetState(lifecycle.StateDraining)
	resp, err = http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("while draining: status = %d, want 503", resp.StatusCode)
	}
}
