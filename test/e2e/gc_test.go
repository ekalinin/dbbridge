package e2e_test

import (
	"net/http"
	"os"
	"testing"
	"time"
)

// TestGC_ExpiresAndDeletes: a result outlives its TTL by at most one GC pass,
// after which both the stored file and the metadata are gone.
func TestGC_ExpiresAndDeletes(t *testing.T) {
	// The GC period is shortened so the pass runs inside the test rather than a
	// minute later. The lock TTL and the pass budget scale with it.
	h := newHarnessWith(t, harnessOptions{gcInterval: 100 * time.Millisecond})

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "sync", "result_ttl_seconds": 1},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.Result == nil || rec.Result.Locator == "" {
		t.Fatalf("result = %+v, want a locator", rec.Result)
	}
	// The FS store's Locator already carries the full path: Writer() sets it to
	// filepath.Join(rootDir, filename), where rootDir is globalResultsDir. So the
	// locator itself is the on-disk path, not a name to join under the root again.
	stored := rec.Result.Locator
	if _, err := os.Stat(stored); err != nil {
		t.Fatalf("result file is missing right after the query: %v", err)
	}

	// The TTL is one second and GC sweeps every 100ms, so the record is gone
	// within roughly 1.1s; three seconds is slack, not an expected wait.
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("query metadata was never collected")
		case <-ticker.C:
			status := get(t, h.baseURL+"/v1/queries/"+rec.ID)
			code := status.StatusCode
			status.Body.Close()
			if code != http.StatusNotFound {
				continue
			}
			if _, err := os.Stat(stored); !os.IsNotExist(err) {
				t.Fatalf("metadata is gone but the result file remains: %v", err)
			}
			return
		}
	}
}

// TestGC_LeavesLiveResultsAlone: a sweep that ran often enough to catch the
// expired record must not touch one that is still inside its TTL.
func TestGC_LeavesLiveResultsAlone(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{gcInterval: 100 * time.Millisecond})

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "sync", "result_ttl_seconds": 3600},
	})
	defer resp.Body.Close()
	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	// Long enough for several sweeps to have run.
	time.Sleep(500 * time.Millisecond)

	status := get(t, h.baseURL+"/v1/queries/"+rec.ID)
	defer status.Body.Close()
	assertStatus(t, status, http.StatusOK)

	dl := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result")
	defer dl.Body.Close()
	assertStatus(t, dl, http.StatusOK)
}

// TestGC_NegativeTTLIsRejected: the TTL is validated before anything runs.
func TestGC_NegativeTTLIsRejected(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync", "result_ttl_seconds": -1},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusBadRequest)
}
