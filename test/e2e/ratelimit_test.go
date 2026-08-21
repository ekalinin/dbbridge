package e2e_test

import (
	"context"
	"net/http"
	"testing"

	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"

	"connectrpc.com/connect"
)

// TestRateLimit_RESTRejectsOverBudget: the concurrency semaphore bounds how
// many queries run at once, but nothing bounded how fast they arrive.
func TestRateLimit_RESTRejectsOverBudget(t *testing.T) {
	// One request per second with no burst headroom: the first call spends the
	// bucket and the second is over budget straight away.
	h := newHarnessWith(t, harnessOptions{rps: 1, burst: 1})

	first := get(t, h.baseURL+"/v1/databases")
	defer first.Body.Close()
	assertStatus(t, first, http.StatusOK)

	second := get(t, h.baseURL+"/v1/databases")
	defer second.Body.Close()
	assertStatus(t, second, http.StatusTooManyRequests)

	var body struct {
		Error     string `json:"error"`
		RequestID string `json:"request_id"`
	}
	decodeJSON(t, second.Body, &body)
	if body.Error == "" {
		t.Error("429 body does not use the error envelope")
	}
}

// TestRateLimit_ProbesAreExempt: the kubelet calls the probes on a fixed
// schedule; they must neither consume a caller's budget nor be starved by it.
func TestRateLimit_ProbesAreExempt(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{rps: 1, burst: 1})

	spend := get(t, h.baseURL+"/v1/databases")
	spend.Body.Close()
	assertStatus(t, spend, http.StatusOK)

	for _, path := range []string{"/healthz", "/readyz", "/healthz", "/readyz"} {
		resp := get(t, h.baseURL+path)
		assertStatus(t, resp, http.StatusOK)
		resp.Body.Close()
	}
}

// TestRateLimit_AppliesWithAuthEnabled: the limiter sits ahead of the auth
// middleware, so an unauthenticated flood is rejected before any credential is
// checked and cannot be used to probe tokens.
func TestRateLimit_AppliesWithAuthEnabled(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens(), rps: 1, burst: 1})

	first := doAuth(t, http.MethodGet, h.baseURL+"/v1/databases", "", "")
	first.Body.Close()
	assertStatus(t, first, http.StatusUnauthorized)

	second := doAuth(t, http.MethodGet, h.baseURL+"/v1/databases", "", "")
	defer second.Body.Close()
	assertStatus(t, second, http.StatusTooManyRequests)
}

// TestRateLimit_ConnectRejectsOverBudget covers the interceptor, which keys by
// peer address because it runs ahead of authentication.
func TestRateLimit_ConnectRejectsOverBudget(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{rps: 1, burst: 1})
	c := h.connectClient(t)

	if _, err := c.ListDatabases(context.Background(), connect.NewRequest(&v1.ListDatabasesRequest{})); err != nil {
		t.Fatalf("first ListDatabases: %v", err)
	}

	_, err := c.ListDatabases(context.Background(), connect.NewRequest(&v1.ListDatabasesRequest{}))
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("second ListDatabases = %v, want resource exhausted", got)
	}
}
