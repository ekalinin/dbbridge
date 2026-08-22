package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// pollForState is the single polling loop behind both pollUntilState and
// ws_test.go's stopWhenRunning. It takes no *testing.T: stopWhenRunning runs
// detached in a goroutine that may outlive the test that started it, and
// calling t.Fatalf from there would panic ("... after Test has completed").
// It reports whether want was observed before the deadline elapsed.
func pollForState(baseURL, queryID, want string, deadline time.Duration) bool {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(deadline)
	for {
		select {
		case <-timeout:
			return false
		case <-ticker.C:
			resp, err := http.Get(fmt.Sprintf("%s/v1/queries/%s", baseURL, queryID))
			if err != nil {
				continue
			}
			var rec queryRecord
			decErr := json.NewDecoder(resp.Body).Decode(&rec)
			resp.Body.Close()
			if decErr != nil {
				continue
			}
			if rec.State == want {
				return true
			}
		}
	}
}

// pollUntilState waits for a query to reach a specific state, failing the
// test if the deadline elapses first.
func pollUntilState(t *testing.T, baseURL, queryID, want string, deadline time.Duration) {
	t.Helper()
	if !pollForState(baseURL, queryID, want, deadline) {
		t.Fatalf("query %s did not reach %s within %v", queryID, want, deadline)
	}
}

// TestCancel_RunningQuery covers the path the "already succeeded" test never
// reached: a query that is genuinely executing when the stop arrives.
func TestCancel_RunningQuery(t *testing.T) {
	h := newHarness(t)
	id := startSlowAsync(t, h)

	pollUntilState(t, h.baseURL, id, "RUNNING", 5*time.Second)

	stop, err := http.Post(h.baseURL+"/v1/queries/"+id+":stop", "application/json", nil)
	if err != nil {
		t.Fatalf("POST stop: %v", err)
	}
	defer stop.Body.Close()
	assertStatus(t, stop, http.StatusOK)

	final := pollUntilTerminal(t, h.baseURL, id, 10*time.Second)
	if final.State != "CANCELED" {
		t.Fatalf("state = %s, want CANCELED", final.State)
	}
	// A canceled query is not a failure: it carries no error object.
	if final.Error != nil {
		t.Errorf("canceled query carries an error: %+v", final.Error)
	}
	if final.Result != nil {
		t.Errorf("canceled query carries a result ref: %+v", final.Result)
	}
}

// TestCancel_ResultIsNotDownloadable: results are only served for SUCCEEDED.
func TestCancel_ResultIsNotDownloadable(t *testing.T) {
	h := newHarness(t)
	id := startSlowAsync(t, h)
	pollUntilState(t, h.baseURL, id, "RUNNING", 5*time.Second)

	stop, err := http.Post(h.baseURL+"/v1/queries/"+id+":stop", "application/json", nil)
	if err != nil {
		t.Fatalf("POST stop: %v", err)
	}
	stop.Body.Close()

	pollUntilTerminal(t, h.baseURL, id, 10*time.Second)

	resp := get(t, h.baseURL+"/v1/queries/"+id+"/result")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusBadRequest)
}

// TestCancel_FreesTheInFlightSlot: the in-flight count backs the can-stop
// answer, so a cancellation that leaks a registry entry keeps a node from ever
// being terminated.
func TestCancel_FreesTheInFlightSlot(t *testing.T) {
	h := newHarness(t)
	id := startSlowAsync(t, h)
	pollUntilState(t, h.baseURL, id, "RUNNING", 5*time.Second)

	busy := get(t, h.baseURL+"/v1/admin/can-stop")
	var busyBody struct {
		CanBeStopped bool `json:"can_be_stopped"`
		InFlight     int  `json:"in_flight"`
	}
	decodeJSON(t, busy.Body, &busyBody)
	busy.Body.Close()
	if busyBody.CanBeStopped || busyBody.InFlight != 1 {
		t.Fatalf("while running: can_be_stopped=%v in_flight=%d, want false/1", busyBody.CanBeStopped, busyBody.InFlight)
	}

	stop, err := http.Post(h.baseURL+"/v1/queries/"+id+":stop", "application/json", nil)
	if err != nil {
		t.Fatalf("POST stop: %v", err)
	}
	stop.Body.Close()
	pollUntilTerminal(t, h.baseURL, id, 10*time.Second)

	idle := get(t, h.baseURL+"/v1/admin/can-stop")
	defer idle.Body.Close()
	var idleBody struct {
		CanBeStopped bool `json:"can_be_stopped"`
		InFlight     int  `json:"in_flight"`
	}
	decodeJSON(t, idle.Body, &idleBody)
	if !idleBody.CanBeStopped || idleBody.InFlight != 0 {
		t.Fatalf("after cancel: can_be_stopped=%v in_flight=%d, want true/0", idleBody.CanBeStopped, idleBody.InFlight)
	}
}

// TestCancel_QueryTimeout: a timeout is a failure with its own code, not a
// cancellation.
func TestCancel_QueryTimeout(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "slowdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync", "timeout_ms": 300},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.State != "FAILED" {
		t.Fatalf("state = %s, want FAILED", rec.State)
	}
	if rec.Error == nil || rec.Error.Code != "QUERY_TIMEOUT" {
		t.Fatalf("error = %+v, want QUERY_TIMEOUT", rec.Error)
	}
}
