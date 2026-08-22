package e2e_test

import (
	"net/http"
	"testing"
)

// TestHarnessOptionsAreApplied is the smoke test for the harness itself: an
// option that is silently dropped would make every test below it pass for the
// wrong reason.
func TestHarnessOptionsAreApplied(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens()})

	resp := get(t, h.baseURL+"/v1/databases")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusUnauthorized)

	if h.grpcURL == "" {
		t.Error("harness has no Connect endpoint")
	}
	if h.qm == nil || h.lm == nil {
		t.Error("harness does not expose the manager and lifecycle")
	}
}
