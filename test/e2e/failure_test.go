package e2e_test

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestFailed_DBExecFailure: a driver failure has to surface as FAILED with a
// typed code, and the driver text - which spells out host, user and password -
// must not reach the caller verbatim.
func TestFailed_DBExecFailure(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "faildb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "sync"},
	})
	defer resp.Body.Close()
	// Submission itself succeeded: the failure happened during execution.
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.State != "FAILED" {
		t.Fatalf("state = %s, want FAILED", rec.State)
	}
	if rec.Error == nil || rec.Error.Code != "DB_EXEC_FAILED" {
		t.Fatalf("error = %+v, want DB_EXEC_FAILED", rec.Error)
	}
	if rec.Result != nil {
		t.Errorf("failed query carries a result ref: %+v", rec.Result)
	}
	if strings.Contains(rec.Error.Message, "hunter2") {
		t.Errorf("error message leaks the driver credentials: %q", rec.Error.Message)
	}
}

// TestFailed_ResultIsNotDownloadable pairs with the cancelled case: results are
// served for SUCCEEDED only.
func TestFailed_ResultIsNotDownloadable(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "faildb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync"},
	})
	defer resp.Body.Close()
	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	dl := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result")
	defer dl.Body.Close()
	assertStatus(t, dl, http.StatusBadRequest)
}

// TestFailed_AsyncFailureIsObservable: an async submission is accepted and only
// then fails, so the failure has to be visible through polling.
func TestFailed_AsyncFailureIsObservable(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "faildb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "async"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusAccepted)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	final := pollUntilTerminal(t, h.baseURL, rec.ID, 10*time.Second)
	if final.State != "FAILED" {
		t.Fatalf("state = %s, want FAILED", final.State)
	}
	if final.Error == nil || final.Error.Code != "DB_EXEC_FAILED" {
		t.Fatalf("error = %+v, want DB_EXEC_FAILED", final.Error)
	}
}

// TestSQLGuard_RejectsWrites: the guard runs before any side effect, so a
// rejected statement never reaches the database and never opens a result file.
func TestSQLGuard_RejectsWrites(t *testing.T) {
	h := newHarness(t)

	cases := []struct {
		name string
		sql  string
	}{
		{"delete", "DELETE FROM users WHERE id = 1"},
		{"insert", "INSERT INTO users (id) VALUES (1)"},
		{"update", "UPDATE users SET name = 'x'"},
		{"ddl", "DROP TABLE users"},
		{"second statement", "SELECT 1; DROP TABLE users"},
		{"session state", "SET search_path = public"},
		{"empty", "   "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
				DatabaseID: "testdb",
				SQL:        tc.sql,
				Options:    map[string]any{"mode": "sync"},
			})
			defer resp.Body.Close()
			assertStatus(t, resp, http.StatusBadRequest)

			// Decode the envelope and check only the `error` field, not the raw
			// body: the body also carries a request_id (chi's RequestID
			// middleware prefixes it with os.Hostname()), and a host whose name
			// happens to contain "users" would otherwise fail this test for a
			// reason that has nothing to do with the SQL guard.
			var envelope struct {
				Error string `json:"error"`
			}
			decodeJSON(t, resp.Body, &envelope)
			// The rejection explains the rule; it never echoes the statement.
			if strings.Contains(envelope.Error, "users") {
				t.Errorf("rejection echoes the statement back: %s", envelope.Error)
			}
		})
	}
}

// TestSQLGuard_AcceptsReads: the guard must not reject the statements the proxy
// exists to run. A keyword hidden in a literal or a comment is still a read.
func TestSQLGuard_AcceptsReads(t *testing.T) {
	h := newHarness(t)

	cases := []string{
		"SELECT id, name FROM users",
		"WITH t AS (SELECT 1) SELECT * FROM t",
		"SELECT 'DROP TABLE users' AS note",
		"SELECT 1 -- DELETE FROM users",
		"EXPLAIN SELECT 1",
		"SHOW TABLES",
	}

	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
				DatabaseID: "testdb",
				SQL:        sql,
				Options:    map[string]any{"mode": "sync"},
			})
			defer resp.Body.Close()
			assertStatus(t, resp, http.StatusOK)
		})
	}
}

// TestSQLGuard_AllowWritesDisablesTheGuard covers the escape hatch operators
// have to ask for explicitly.
func TestSQLGuard_AllowWritesDisablesTheGuard(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{allowWrites: true})

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "DELETE FROM users WHERE id = 1",
		Options:    map[string]any{"mode": "sync"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED", rec.State)
	}
}
