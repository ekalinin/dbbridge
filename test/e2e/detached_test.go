package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"

	"connectrpc.com/connect"
)

// The tests in this file cover I1, the invariant the whole proxy exists for:
// "The query lifecycle is NOT bound to the incoming connection ... Disconnecting
// the consumer does not cancel the query" (spec §1).
//
// Everything else about I1 was verified structurally - rest/tracing_test.go
// asserts the execution span links the submitter rather than descending from
// it - which proves the spans are detached, not the execution. These drop the
// connection while the query is still inside the driver and then require the
// query to finish anyway.
//
// The gated driver behind "gatedb" is what makes the timing deterministic: the
// query cannot progress past Exec until the test says so, and by then the
// request that submitted it is long gone. If the execution context were derived
// from the request, gatePool.Exec would take its ctx.Done branch and the query
// would end CANCELED instead of SUCCEEDED.

// TestDetached_AsyncQueryOutlivesTheSubmitRequest covers the plain async case.
// The handler answers 202 and returns, at which point net/http cancels the
// request context - while the query is still parked in the driver.
func TestDetached_AsyncQueryOutlivesTheSubmitRequest(t *testing.T) {
	h := newHarness(t)
	gate := armGate(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "gatedb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "async"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusAccepted)

	var submitted queryRecord
	decodeJSON(t, resp.Body, &submitted)

	// The submitting request is complete and its context canceled; the query is
	// still blocked inside Exec.
	gate.WaitEntered(t)
	http.DefaultClient.CloseIdleConnections()

	rec := waitForState(t, h.baseURL, submitted.ID, "RUNNING", 5*time.Second)
	if rec.State != "RUNNING" {
		t.Fatalf("state = %s, want RUNNING while the gate is held", rec.State)
	}

	gate.Release()

	final := pollUntilTerminal(t, h.baseURL, submitted.ID, 10*time.Second)
	if final.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED: the query did not survive the submitting request", final.State)
	}
	if final.Result == nil || final.Result.RowCount != 2 {
		t.Fatalf("result = %+v, want the full two-row result", final.Result)
	}
}

// TestDetached_SyncQueryOutlivesAnAbortedRequest is the harder case: mode=sync
// holds the connection open for the whole execution, so aborting it is exactly
// the consumer restarting mid-query.
//
// The query ID is recovered through the idempotency key rather than from the
// aborted response, which is how a restarted consumer would find its own query
// again (I3) - and is the only way to learn the ID when the response never
// arrives.
func TestDetached_SyncQueryOutlivesAnAbortedRequest(t *testing.T) {
	h := newHarness(t)
	gate := armGate(t)

	const idemKey = "detached-sync-1"
	body, err := json.Marshal(startQueryPayload{
		DatabaseID: "gatedb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "sync"},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+"/v1/queries", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idemKey)

	// A dedicated client so aborting cannot disturb connections other tests
	// share through http.DefaultClient.
	client := &http.Client{Transport: &http.Transport{}}
	errCh := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		errCh <- err
	}()

	// Abort only once the query is genuinely in flight, otherwise the test could
	// pass by cancelling before anything started.
	gate.WaitEntered(t)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("the sync submission returned normally, so the connection was never dropped mid-query")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the aborted request never returned")
	}
	client.CloseIdleConnections()

	// The consumer is gone. Release the database and let the query run to
	// completion with nobody listening.
	gate.Release()

	queryID := reclaimByIdempotencyKey(t, h.baseURL, idemKey)
	final := pollUntilTerminal(t, h.baseURL, queryID, 10*time.Second)
	if final.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED: aborting the sync request canceled the query", final.State)
	}

	// The result has to be there too: surviving as a record but losing the bytes
	// would satisfy the letter of I1 and none of its point.
	dl := get(t, h.baseURL+"/v1/queries/"+queryID+"/result")
	defer dl.Body.Close()
	assertStatus(t, dl, http.StatusOK)
	payload, err := io.ReadAll(dl.Body)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(string(payload)), "\n") + 1; lines != 2 {
		t.Fatalf("result has %d lines, want 2:\n%s", lines, payload)
	}
}

// TestDetached_TimeoutStillAppliesToADetachedQuery is the other half of I1: the
// execution context is decoupled from the request but still bounded by
// options.timeout (spec §6 step 5), so a detached query cannot run for ever.
func TestDetached_TimeoutStillAppliesToADetachedQuery(t *testing.T) {
	h := newHarness(t)
	gate := armGate(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "gatedb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "async", "timeout_ms": 300},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusAccepted)

	var submitted queryRecord
	decodeJSON(t, resp.Body, &submitted)

	gate.WaitEntered(t)

	// The gate is never released: the deadline is what ends this query.
	final := pollUntilTerminal(t, h.baseURL, submitted.ID, 10*time.Second)
	if final.State != "FAILED" {
		t.Fatalf("state = %s, want FAILED once the timeout expires", final.State)
	}
	if final.Error == nil || final.Error.Code != "QUERY_TIMEOUT" {
		t.Fatalf("error = %+v, want QUERY_TIMEOUT", final.Error)
	}
}

// reclaimByIdempotencyKey re-submits under an existing key and returns the query
// ID the server hands back, which is how a restarted consumer finds the query it
// lost the response to.
func reclaimByIdempotencyKey(t *testing.T, baseURL, key string) string {
	t.Helper()
	resp := postJSONWithHeader(t, baseURL+"/v1/queries", "Idempotency-Key", key, startQueryPayload{
		DatabaseID: "gatedb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "async"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("reclaim by idempotency key: status %d: %s", resp.StatusCode, body)
	}
	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.ID == "" {
		t.Fatal("reclaim by idempotency key returned no query ID")
	}
	return rec.ID
}

// waitForState polls until the query reports want, or returns the last record
// seen when the deadline runs out.
func waitForState(t *testing.T, baseURL, queryID, want string, deadline time.Duration) queryRecord {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(deadline)
	var last queryRecord
	for {
		select {
		case <-timeout:
			return last
		case <-ticker.C:
			resp := get(t, baseURL+"/v1/queries/"+queryID)
			decodeJSON(t, resp.Body, &last)
			resp.Body.Close()
			if last.State == want {
				return last
			}
		}
	}
}

// TestBodyLimit_BothTransportsRejectAnOversizedSubmission covers §9 Limits:
// "server.max_request_bytes caps a request body". The cap used to live only in
// the REST handler, so the SQL text of a Connect submission was bounded by
// nothing at all - the two transports mean the same API and have to enforce the
// same limit. The codes differ because the protocols do: 413 on HTTP,
// ResourceExhausted on Connect.
func TestBodyLimit_BothTransportsRejectAnOversizedSubmission(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{maxRequestBytes: 512})
	oversized := "SELECT '" + strings.Repeat("x", 4096) + "'"

	t.Run("rest", func(t *testing.T) {
		resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
			DatabaseID: "testdb",
			SQL:        oversized,
		})
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("status = %d, want 413", resp.StatusCode)
		}
	})

	t.Run("connect", func(t *testing.T) {
		client := h.connectClient(t)
		_, err := client.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
			DatabaseId: "testdb",
			Sql:        oversized,
		}))
		if err == nil {
			t.Fatal("Connect accepted a submission past server.max_request_bytes")
		}
		if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
			t.Errorf("code = %v, want ResourceExhausted", got)
		}
	})

	// A submission inside the limit still goes through on both, so the cap is a
	// limit and not an outage.
	t.Run("within the limit", func(t *testing.T) {
		resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
			DatabaseID: "testdb",
			SQL:        "SELECT id, name FROM users",
		})
		defer resp.Body.Close()
		assertStatus(t, resp, http.StatusAccepted)

		client := h.connectClient(t)
		if _, err := client.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
			DatabaseId: "testdb",
			Sql:        "SELECT id, name FROM users",
		})); err != nil {
			t.Errorf("Connect rejected a submission inside the limit: %v", err)
		}
	})
}
