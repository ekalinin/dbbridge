package rest_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dbbridge/internal/lifecycle"
	"dbbridge/internal/testutil"
	"dbbridge/internal/transport/rest"
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
