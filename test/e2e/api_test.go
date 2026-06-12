package e2e_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func postJSONWithHeader(t *testing.T, url, headerName, headerValue string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerName, headerValue)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func decodeJSON(t *testing.T, r io.Reader, dst any) {
	t.Helper()
	if err := json.NewDecoder(r).Decode(dst); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

func assertStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status %d, got %d: %s", want, resp.StatusCode, body)
	}
}

// startQueryPayload mirrors rest.StartQueryPayload for tests.
type startQueryPayload struct {
	DatabaseID string         `json:"database_id"`
	SQL        string         `json:"sql"`
	Options    map[string]any `json:"options"`
}

// queryRecord is the subset of domain.QueryRecord we care about in tests.
type queryRecord struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Stats struct {
		RowsRead     int64 `json:"rows_read"`
		BytesWritten int64 `json:"bytes_written"`
	} `json:"stats"`
	Result *struct {
		Backend  string `json:"backend"`
		RowCount int64  `json:"row_count"`
		Format   string `json:"format"`
	} `json:"result"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// pollUntilTerminal polls GET /v1/queries/{id} until the query reaches a
// terminal state or the deadline is exceeded.
func pollUntilTerminal(t *testing.T, baseURL, queryID string, deadline time.Duration) queryRecord {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(deadline)
	for {
		select {
		case <-timeout:
			t.Fatalf("query %s did not reach terminal state within %v", queryID, deadline)
		case <-ticker.C:
			resp := get(t, fmt.Sprintf("%s/v1/queries/%s", baseURL, queryID))
			var rec queryRecord
			decodeJSON(t, resp.Body, &rec)
			resp.Body.Close()
			switch rec.State {
			case "SUCCEEDED", "FAILED", "CANCELED":
				return rec
			}
		}
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestHealthz(t *testing.T) {
	h := newHarness(t)
	resp := get(t, h.baseURL+"/healthz")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK" {
		t.Fatalf("expected body 'OK', got %q", body)
	}
}

func TestReadyz(t *testing.T) {
	h := newHarness(t)
	resp := get(t, h.baseURL+"/readyz")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)
}

func TestListDatabases(t *testing.T) {
	h := newHarness(t)
	resp := get(t, h.baseURL+"/v1/databases")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var dbs []struct {
		ID     string `json:"id"`
		Engine string `json:"engine"`
	}
	decodeJSON(t, resp.Body, &dbs)

	if len(dbs) != 1 {
		t.Fatalf("expected 1 database, got %d", len(dbs))
	}
	if dbs[0].ID != "testdb" {
		t.Errorf("expected database ID 'testdb', got %q", dbs[0].ID)
	}
}

func TestStartQueryAsyncAndPoll(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "async"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusAccepted)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.ID == "" {
		t.Fatal("expected non-empty query_id")
	}

	final := pollUntilTerminal(t, h.baseURL, rec.ID, 10*time.Second)
	if final.State != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %s", final.State)
	}
	if final.Stats.RowsRead != 2 {
		t.Errorf("expected 2 rows_read, got %d", final.Stats.RowsRead)
	}
}

func TestStartQuerySync(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync", "result_format": "jsonl"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.State != "SUCCEEDED" {
		t.Fatalf("sync query not SUCCEEDED immediately, got %s", rec.State)
	}
	if rec.Result == nil {
		t.Fatal("sync query has no Result ref")
	}
}

func TestDownloadResult_JSONL(t *testing.T) {
	h := newHarness(t)

	// Start a sync query so result is immediately available.
	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM t",
		Options:    map[string]any{"mode": "sync", "result_format": "jsonl"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	// Download the result.
	dlResp := get(t, fmt.Sprintf("%s/v1/queries/%s/result", h.baseURL, rec.ID))
	defer dlResp.Body.Close()
	assertStatus(t, dlResp, http.StatusOK)

	scanner := bufio.NewScanner(dlResp.Body)
	var rows []map[string]any
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("parse JSONL line %q: %v", line, err)
		}
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 JSONL rows, got %d", len(rows))
	}
	if rows[0]["name"] != "alice" {
		t.Errorf("row 0 name: want 'alice', got %v", rows[0]["name"])
	}
	if rows[1]["name"] != "bob" {
		t.Errorf("row 1 name: want 'bob', got %v", rows[1]["name"])
	}
}

func TestDownloadResult_CSV(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM t",
		Options:    map[string]any{"mode": "sync", "result_format": "csv"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	dlResp := get(t, fmt.Sprintf("%s/v1/queries/%s/result", h.baseURL, rec.ID))
	defer dlResp.Body.Close()
	assertStatus(t, dlResp, http.StatusOK)

	body, _ := io.ReadAll(dlResp.Body)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 3 { // header + 2 rows
		t.Fatalf("expected 3 CSV lines (header+2 rows), got %d: %s", len(lines), body)
	}
	if lines[0] != "id,name" {
		t.Errorf("CSV header: want 'id,name', got %q", lines[0])
	}
}

func TestGetQueryStats(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	statsResp := get(t, fmt.Sprintf("%s/v1/queries/%s/stats", h.baseURL, rec.ID))
	defer statsResp.Body.Close()
	assertStatus(t, statsResp, http.StatusOK)

	var stats struct {
		RowsRead int64 `json:"rows_read"`
	}
	decodeJSON(t, statsResp.Body, &stats)
	if stats.RowsRead != 2 {
		t.Errorf("expected rows_read=2, got %d", stats.RowsRead)
	}
}

func TestIdempotency(t *testing.T) {
	h := newHarness(t)

	payload := startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync"},
	}

	resp1 := postJSONWithHeader(t, h.baseURL+"/v1/queries", "Idempotency-Key", "key-abc", payload)
	defer resp1.Body.Close()
	var rec1 queryRecord
	decodeJSON(t, resp1.Body, &rec1)

	resp2 := postJSONWithHeader(t, h.baseURL+"/v1/queries", "Idempotency-Key", "key-abc", payload)
	defer resp2.Body.Close()
	var rec2 queryRecord
	decodeJSON(t, resp2.Body, &rec2)

	if rec1.ID != rec2.ID {
		t.Errorf("idempotency failed: first=%q second=%q", rec1.ID, rec2.ID)
	}
}

func TestStopQuery_AlreadySucceeded(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync"},
	})
	defer resp.Body.Close()

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	// Stopping a succeeded query is a no-op, should return 200.
	stopResp, err := http.Post(
		fmt.Sprintf("%s/v1/queries/%s:stop", h.baseURL, rec.ID),
		"application/json", nil,
	)
	if err != nil {
		t.Fatalf("POST stop: %v", err)
	}
	defer stopResp.Body.Close()
	assertStatus(t, stopResp, http.StatusOK)
}

func TestGetStatus_NotFound(t *testing.T) {
	h := newHarness(t)
	resp := get(t, h.baseURL+"/v1/queries/does-not-exist")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusNotFound)
}

func TestStartQuery_UnknownDatabase(t *testing.T) {
	h := newHarness(t)
	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "no-such-db",
		SQL:        "SELECT 1",
		Options:    map[string]any{},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusInternalServerError)
}

func TestStartQuery_MissingFields(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name    string
		payload startQueryPayload
	}{
		{"missing database_id", startQueryPayload{SQL: "SELECT 1"}},
		{"missing sql", startQueryPayload{DatabaseID: "testdb"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postJSON(t, h.baseURL+"/v1/queries", tc.payload)
			defer resp.Body.Close()
			assertStatus(t, resp, http.StatusBadRequest)
		})
	}
}

func TestCanIBeStopped(t *testing.T) {
	h := newHarness(t)
	resp := get(t, h.baseURL+"/v1/admin/can-stop")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var body struct {
		CanBeStopped bool `json:"can_be_stopped"`
		InFlight     int  `json:"in_flight"`
	}
	decodeJSON(t, resp.Body, &body)
	if !body.CanBeStopped {
		t.Error("expected can_be_stopped=true when no queries are running")
	}
	if body.InFlight != 0 {
		t.Errorf("expected in_flight=0, got %d", body.InFlight)
	}
}

func TestReloadConfig(t *testing.T) {
	h := newHarness(t)
	resp, err := http.Post(h.baseURL+"/v1/admin/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("POST reload: %v", err)
	}
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var body map[string]any
	decodeJSON(t, resp.Body, &body)
	if body["success"] != true {
		t.Errorf("expected success=true, got %v", body["success"])
	}
}

func TestMetrics(t *testing.T) {
	h := newHarness(t)
	resp := get(t, h.baseURL+"/metrics")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "go_") {
		t.Error("expected Prometheus metrics in response")
	}
}
