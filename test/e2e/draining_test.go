package e2e_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/lifecycle"
)

// canStopBody is the shape of GET /v1/admin/can-stop.
type canStopBody struct {
	CanBeStopped  bool   `json:"can_be_stopped"`
	InFlight      int    `json:"in_flight"`
	InstanceState string `json:"instance_state"`
}

func readCanStop(t *testing.T, h *testHarness) canStopBody {
	t.Helper()
	resp := get(t, h.baseURL+"/v1/admin/can-stop")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)
	var body canStopBody
	decodeJSON(t, resp.Body, &body)
	return body
}

// TestDraining_ReadinessAndAdmission: SIGTERM puts the instance into DRAINING,
// which takes it out of the load balancer rotation and closes admission while
// the queries it already owns finish.
func TestDraining_ReadinessAndAdmission(t *testing.T) {
	h := newHarness(t)

	ready := get(t, h.baseURL+"/readyz")
	assertStatus(t, ready, http.StatusOK)
	ready.Body.Close()

	// What main.go does on SIGTERM, minus the listener shutdown.
	h.lm.SetState(lifecycle.StateDraining)
	h.qm.Drain()

	notReady := get(t, h.baseURL+"/readyz")
	defer notReady.Body.Close()
	assertStatus(t, notReady, http.StatusServiceUnavailable)
	body, _ := io.ReadAll(notReady.Body)
	if string(body) != "NOT READY" {
		t.Errorf("readyz body = %q, want NOT READY", body)
	}

	// Liveness stays green: the process is healthy, just not taking traffic.
	live := get(t, h.baseURL+"/healthz")
	defer live.Body.Close()
	assertStatus(t, live, http.StatusOK)

	rejected := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync"},
	})
	defer rejected.Body.Close()
	assertStatus(t, rejected, http.StatusServiceUnavailable)
}

// TestDraining_ReadsKeepWorking: draining rejects new work, it does not take
// the API away from clients polling the queries that are still running.
func TestDraining_ReadsKeepWorking(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "sync"},
	})
	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	resp.Body.Close()

	h.lm.SetState(lifecycle.StateDraining)
	h.qm.Drain()

	status := get(t, h.baseURL+"/v1/queries/"+rec.ID)
	defer status.Body.Close()
	assertStatus(t, status, http.StatusOK)

	result := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result")
	defer result.Body.Close()
	assertStatus(t, result, http.StatusOK)

	list := get(t, h.baseURL+"/v1/databases")
	defer list.Body.Close()
	assertStatus(t, list, http.StatusOK)
}

// TestDraining_AdvancesToStoppable: the orchestrator polls can-stop, and the
// instance may only be terminated once the queries it owns have finished.
func TestDraining_AdvancesToStoppable(t *testing.T) {
	h := newHarness(t)
	id := startSlowAsync(t, h)
	pollUntilState(t, h.baseURL, id, "RUNNING", 5*time.Second)

	h.lm.SetState(lifecycle.StateDraining)
	h.qm.Drain()

	busy := readCanStop(t, h)
	if busy.CanBeStopped {
		t.Error("can_be_stopped is true while a query is still running")
	}
	if busy.InFlight != 1 {
		t.Errorf("in_flight = %d, want 1", busy.InFlight)
	}
	if busy.InstanceState != string(lifecycle.StateDraining) {
		t.Errorf("instance_state = %q, want DRAINING", busy.InstanceState)
	}

	stop, err := http.Post(h.baseURL+"/v1/queries/"+id+":stop", "application/json", nil)
	if err != nil {
		t.Fatalf("POST stop: %v", err)
	}
	stop.Body.Close()
	pollUntilTerminal(t, h.baseURL, id, 10*time.Second)

	quiesced := readCanStop(t, h)
	if !quiesced.CanBeStopped {
		t.Error("can_be_stopped is false after the last query finished")
	}
	if quiesced.InFlight != 0 {
		t.Errorf("in_flight = %d, want 0", quiesced.InFlight)
	}
	if quiesced.InstanceState != string(lifecycle.StateStoppable) {
		t.Errorf("instance_state = %q, want STOPPABLE", quiesced.InstanceState)
	}

	// Readiness stays red in STOPPABLE, right up to termination.
	notReady := get(t, h.baseURL+"/readyz")
	defer notReady.Body.Close()
	assertStatus(t, notReady, http.StatusServiceUnavailable)
}
