package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/rest"
	"github.com/ekalinin/dbbridge/internal/transport/ws"

	"github.com/coder/websocket"
)

func TestREST_StartQuery_DrainingReturns503(t *testing.T) {
	svc, lm := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
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
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
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

// TestREST_ErrorsAreTypedAndSanitized: every failure except draining used to be
// a 500 whose body carried the wrapped driver error, DSN included.
func TestREST_ErrorsAreTypedAndSanitized(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	cases := []struct {
		name string
		body string
		want int
	}{
		{"unknown database", `{"database_id":"nope","sql":"SELECT 1"}`, http.StatusNotFound},
		{"bad format", `{"database_id":"testdb","sql":"SELECT 1","options":{"result_format":"avro"}}`, http.StatusBadRequest},
		{"bad mode", `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"SYNC"}}`, http.StatusBadRequest},
		{"write statement", `{"database_id":"testdb","sql":"DELETE FROM t"}`, http.StatusBadRequest},
		{"malformed json", `{`, http.StatusBadRequest},
		{"missing sql", `{"database_id":"testdb"}`, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+"/v1/queries", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("POST /v1/queries: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want %d: %s", resp.StatusCode, tc.want, body)
			}

			body, _ := io.ReadAll(resp.Body)
			lower := strings.ToLower(string(body))
			for _, leak := range []string{"dsn", "password", "postgres://", "fake@"} {
				if strings.Contains(lower, leak) {
					t.Errorf("response leaks %q: %s", leak, body)
				}
			}
		})
	}
}

func TestREST_UnknownQueryIs404(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	for _, path := range []string{"/v1/queries/nope", "/v1/queries/nope/stats", "/v1/queries/nope/result"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, resp.StatusCode)
		}
	}
}

// TestREST_RequestBodyLimit: the SQL text was read without any bound.
func TestREST_RequestBodyLimit(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{MaxRequestBytes: 256}).Handler())
	t.Cleanup(ts.Close)

	body := `{"database_id":"testdb","sql":"SELECT '` + strings.Repeat("x", 1024) + `'"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/queries: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}

// TestREST_DownloadOutlivesRequestTimeout is the regression for the shared
// middleware.Timeout: it covered the whole router, so downloading a large
// result - the product's main scenario - was cut off after a minute. A backend
// that honours its context, which is what S3 does, saw the cancellation and
// truncated the stream.
func TestREST_DownloadOutlivesRequestTimeout(t *testing.T) {
	svc, _ := testutil.NewService(t)
	store := registerSlowStore(t)

	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{
		// Far shorter than the download takes.
		RequestTimeout: 100 * time.Millisecond,
	}).Handler())
	t.Cleanup(ts.Close)

	body := `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync","storage_backend":"` + slowBackend + `"}}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/queries: %v", err)
	}
	var rec struct {
		ID     string `json:"id"`
		Result struct {
			SizeBytes int64 `json:"size_bytes"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if rec.ID == "" {
		t.Fatal("no query id in the response")
	}

	dl, err := http.Get(ts.URL + "/v1/queries/" + rec.ID + "/result")
	if err != nil {
		t.Fatalf("GET result: %v", err)
	}
	defer dl.Body.Close()
	if dl.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", dl.StatusCode)
	}

	got, err := io.ReadAll(dl.Body)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if int64(len(got)) != rec.Result.SizeBytes {
		t.Errorf("read %d bytes, the result holds %d: the download was truncated", len(got), rec.Result.SizeBytes)
	}

	// The route carries no deadline at all, which is the property that keeps a
	// context-honouring backend from aborting mid-stream.
	if store.readerHadDeadline() {
		t.Error("the download handler ran with a request deadline")
	}
}

// TestREST_ResponseMatchesOpenAPI pins the duration units and field names the
// OpenAPI document promises. The domain model was serialized straight to the
// wire, so responses carried `timeout` and `db_exec_duration` in nanoseconds.
func TestREST_ResponseMatchesOpenAPI(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	body := `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync","timeout_ms":5000,"result_ttl_seconds":60}}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/queries: %v", err)
	}
	defer resp.Body.Close()

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	opts, _ := got["options"].(map[string]any)
	if opts == nil {
		t.Fatal("response has no options object")
	}
	if v, ok := opts["timeout_ms"].(float64); !ok || int64(v) != 5000 {
		t.Errorf("options.timeout_ms = %v, want 5000", opts["timeout_ms"])
	}
	if v, ok := opts["result_ttl_seconds"].(float64); !ok || int64(v) != 60 {
		t.Errorf("options.result_ttl_seconds = %v, want 60", opts["result_ttl_seconds"])
	}
	for _, gone := range []string{"timeout", "result_ttl"} {
		if _, ok := opts[gone]; ok {
			t.Errorf("options still carries the domain field %q", gone)
		}
	}

	stats, _ := got["stats"].(map[string]any)
	if stats == nil {
		t.Fatal("response has no stats object")
	}
	for _, want := range []string{"db_exec_duration_ms", "storage_write_duration_ms", "total_duration_ms"} {
		if _, ok := stats[want]; !ok {
			t.Errorf("stats is missing %q", want)
		}
	}
	if _, ok := stats["db_exec_duration"]; ok {
		t.Error("stats still carries the domain field db_exec_duration")
	}
}

// TestREST_RangeRequests covers 206 and the unsatisfiable case, which used to
// answer 206 with a "bytes 0--1/*" header.
func TestREST_RangeRequests(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{}).Handler())
	t.Cleanup(ts.Close)

	body := `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync"}}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/queries: %v", err)
	}
	var rec struct {
		ID     string `json:"id"`
		Result struct {
			SizeBytes int64 `json:"size_bytes"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()

	rangeGet := func(spec string) *http.Response {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/queries/"+rec.ID+"/result", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Range", spec)
		out, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET result: %v", err)
		}
		return out
	}

	partial := rangeGet("bytes=0-3")
	defer partial.Body.Close()
	if partial.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206", partial.StatusCode)
	}
	wantRange := fmt.Sprintf("bytes 0-3/%d", rec.Result.SizeBytes)
	if got := partial.Header.Get("Content-Range"); got != wantRange {
		t.Errorf("Content-Range = %q, want %q", got, wantRange)
	}

	beyond := rangeGet(fmt.Sprintf("bytes=%d-", rec.Result.SizeBytes+10))
	defer beyond.Body.Close()
	if beyond.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("out-of-range status = %d, want 416", beyond.StatusCode)
	}
	if got := beyond.Header.Get("Content-Range"); got != fmt.Sprintf("bytes */%d", rec.Result.SizeBytes) {
		t.Errorf("unsatisfiable Content-Range = %q", got)
	}
}

func testAuth(t *testing.T) *authn.Authenticator {
	t.Helper()
	a, err := authn.New([]authn.TokenSpec{
		{Subject: "alice", Value: "alice-token", Scopes: []string{"read", "write"}},
		{Subject: "bob", Value: "bob-token", Scopes: []string{"read", "write"}},
		{Subject: "watcher", Value: "read-token", Scopes: []string{"read"}},
		{Subject: "writer", Value: "writer-token", Scopes: []string{"write"}},
		{Subject: "root", Value: "admin-token", Scopes: []string{"admin"}},
	})
	if err != nil {
		t.Fatalf("authn.New: %v", err)
	}
	return a
}

func do(t *testing.T, method, url, token, body string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// TestREST_RequiresCredentials: there was no authentication middleware at all,
// so anyone who could reach the port could run SQL, stop other people's queries
// and reload the process.
func TestREST_RequiresCredentials(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{Auth: testAuth(t)}).Handler())
	t.Cleanup(ts.Close)

	cases := []struct {
		name   string
		method string
		path   string
		token  string
		body   string
		want   int
	}{
		{"submit without a token", http.MethodPost, "/v1/queries", "", `{"database_id":"testdb","sql":"SELECT 1"}`, http.StatusUnauthorized},
		{"submit with a bad token", http.MethodPost, "/v1/queries", "nope", `{"database_id":"testdb","sql":"SELECT 1"}`, http.StatusUnauthorized},
		{"submit with a read-only token", http.MethodPost, "/v1/queries", "read-token", `{"database_id":"testdb","sql":"SELECT 1"}`, http.StatusForbidden},
		{"list without a token", http.MethodGet, "/v1/databases", "", "", http.StatusUnauthorized},
		{"list with a read token", http.MethodGet, "/v1/databases", "read-token", "", http.StatusOK},
		{"reload with a write token", http.MethodPost, "/v1/admin/reload", "alice-token", "", http.StatusForbidden},
		{"reload with an admin token", http.MethodPost, "/v1/admin/reload", "admin-token", "", http.StatusOK},
		// /metrics enumerates every configured db_id and the traffic volume per
		// database, and admin_addr is optional, so the default deployment has it
		// on the public listener.
		{"metrics without a token", http.MethodGet, "/metrics", "", "", http.StatusUnauthorized},
		{"metrics with a read token", http.MethodGet, "/metrics", "read-token", "", http.StatusForbidden},
		{"metrics with an admin token", http.MethodGet, "/metrics", "admin-token", "", http.StatusOK},
		// A write token has to be able to poll the query it submitted.
		{"status with a write-only token", http.MethodGet, "/v1/queries/nope", "writer-token", "", http.StatusNotFound},
		{"ws without a token", http.MethodGet, "/v1/ws", "", "", http.StatusUnauthorized},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := do(t, tc.method, ts.URL+tc.path, tc.token, tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("status = %d, want %d: %s", resp.StatusCode, tc.want, body)
			}
		})
	}
}

func TestREST_ProbesStayOpen(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{Auth: testAuth(t)}).Handler())
	t.Cleanup(ts.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp := do(t, http.MethodGet, ts.URL+path, "", "")
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s without a token: status = %d, want 200", path, resp.StatusCode)
		}
	}
}

// TestREST_QueriesAreBoundToTheirSubject: knowing a query ID used to be enough
// to read anyone's SQL, status and result.
func TestREST_QueriesAreBoundToTheirSubject(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{Auth: testAuth(t)}).Handler())
	t.Cleanup(ts.Close)

	resp := do(t, http.MethodPost, ts.URL+"/v1/queries", "alice-token",
		`{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync"}}`)
	var rec struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if rec.ID == "" {
		t.Fatal("no query id in the response")
	}

	for _, path := range []string{"/v1/queries/" + rec.ID, "/v1/queries/" + rec.ID + "/stats", "/v1/queries/" + rec.ID + "/result"} {
		// The owner reads its own query.
		own := do(t, http.MethodGet, ts.URL+path, "alice-token", "")
		own.Body.Close()
		if own.StatusCode != http.StatusOK {
			t.Errorf("owner GET %s: status = %d, want 200", path, own.StatusCode)
		}

		// A different subject must not, and must not learn the ID exists.
		foreign := do(t, http.MethodGet, ts.URL+path, "bob-token", "")
		foreign.Body.Close()
		if foreign.StatusCode != http.StatusNotFound {
			t.Errorf("foreign GET %s: status = %d, want 404", path, foreign.StatusCode)
		}

		// Admin sees everything.
		root := do(t, http.MethodGet, ts.URL+path, "admin-token", "")
		root.Body.Close()
		if root.StatusCode != http.StatusOK {
			t.Errorf("admin GET %s: status = %d, want 200", path, root.StatusCode)
		}
	}

	stop := do(t, http.MethodPost, ts.URL+"/v1/queries/"+rec.ID+":stop", "bob-token", "")
	stop.Body.Close()
	if stop.StatusCode != http.StatusNotFound {
		t.Errorf("foreign stop: status = %d, want 404", stop.StatusCode)
	}
}

// TestREST_SeparateAdminListener keeps /metrics and /v1/admin off the public
// router: /metrics enumerates every configured db_id. Moving them to their own
// listener is network isolation, not authorization, so the admin scope is still
// required there.
func TestREST_SeparateAdminListener(t *testing.T) {
	svc, _ := testutil.NewService(t)
	srv := rest.NewServer(svc, rest.Options{SeparateAdmin: true, Auth: testAuth(t)})

	public := httptest.NewServer(srv.Handler())
	t.Cleanup(public.Close)

	adminHandler := srv.AdminHandler()
	if adminHandler == nil {
		t.Fatal("AdminHandler is nil although SeparateAdmin is set")
	}
	admin := httptest.NewServer(adminHandler)
	t.Cleanup(admin.Close)

	for _, path := range []string{"/metrics", "/v1/admin/can-stop"} {
		pub := do(t, http.MethodGet, public.URL+path, "admin-token", "")
		pub.Body.Close()
		if pub.StatusCode != http.StatusNotFound {
			t.Errorf("public GET %s: status = %d, want 404", path, pub.StatusCode)
		}

		anon := do(t, http.MethodGet, admin.URL+path, "", "")
		anon.Body.Close()
		if anon.StatusCode != http.StatusUnauthorized {
			t.Errorf("admin GET %s without a token: status = %d, want 401", path, anon.StatusCode)
		}

		reader := do(t, http.MethodGet, admin.URL+path, "read-token", "")
		reader.Body.Close()
		if reader.StatusCode != http.StatusForbidden {
			t.Errorf("admin GET %s with a read token: status = %d, want 403", path, reader.StatusCode)
		}

		adm := do(t, http.MethodGet, admin.URL+path, "admin-token", "")
		adm.Body.Close()
		if adm.StatusCode != http.StatusOK {
			t.Errorf("admin GET %s: status = %d, want 200", path, adm.StatusCode)
		}
	}
}

// TestREST_IdempotencyKeyIsScopedToTheSubject: the key used to be global per
// database and StartQuery is the one read path that does not go through
// service.authorized, so a caller that sent somebody else's key got their whole
// record back - SQL text, stats, owner and result locator - and its own SQL was
// never run.
func TestREST_IdempotencyKeyIsScopedToTheSubject(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{Auth: testAuth(t)}).Handler())
	t.Cleanup(ts.Close)

	submit := func(token, sql string) (string, string) {
		t.Helper()
		body := fmt.Sprintf(`{"database_id":"testdb","sql":%q,"options":{"mode":"sync"}}`, sql)
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/queries", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", "shared-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			raw, _ := io.ReadAll(resp.Body)
			t.Fatalf("submit as %s: status = %d: %s", token, resp.StatusCode, raw)
		}
		var rec struct {
			ID  string `json:"id"`
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return rec.ID, rec.SQL
	}

	aliceID, _ := submit("alice-token", "SELECT secret_from_alice")
	bobID, bobSQL := submit("bob-token", "SELECT bobs_own_sql")

	if bobID == aliceID {
		t.Error("bob's submission returned alice's query id")
	}
	if bobSQL != "SELECT bobs_own_sql" {
		t.Errorf("bob got sql = %q, want his own", bobSQL)
	}

	// The same subject still gets its own query back: I3 has to keep holding
	// inside a subject.
	againID, _ := submit("alice-token", "SELECT secret_from_alice")
	if againID != aliceID {
		t.Errorf("a repeated submission returned %s, want %s", againID, aliceID)
	}
}

// TestREST_AuthErrorsUseTheApiEnvelope: 401 and 403 used to come out of
// http.Error as text/plain, so a client could not parse them with the code it
// uses for every other error and the request ID was lost.
func TestREST_AuthErrorsUseTheApiEnvelope(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{Auth: testAuth(t)}).Handler())
	t.Cleanup(ts.Close)

	for _, tc := range []struct {
		name  string
		token string
		want  int
	}{
		{"unauthenticated", "", http.StatusUnauthorized},
		{"forbidden", "read-token", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := do(t, http.MethodPost, ts.URL+"/v1/queries", tc.token, `{"database_id":"testdb","sql":"SELECT 1"}`)
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			if v := resp.Header.Get("Vary"); !strings.Contains(v, "Authorization") {
				t.Errorf("Vary = %q, want it to contain Authorization", v)
			}
			var body struct {
				Error     string `json:"error"`
				RequestID string `json:"request_id"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Error == "" || body.RequestID == "" {
				t.Errorf("envelope = %+v, want both fields set", body)
			}
		})
	}
}

// TestREST_WebSocketAcceptsASubprotocolCredential: the browser WebSocket API
// cannot set the Authorization header, so gating /v1/ws on it would close the
// route to exactly the clients the Origin check exists for.
func TestREST_WebSocketAcceptsASubprotocolCredential(t *testing.T) {
	svc, _ := testutil.NewService(t)
	ts := httptest.NewServer(rest.NewServer(svc, rest.Options{Auth: testAuth(t)}).Handler())
	t.Cleanup(ts.Close)

	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/ws"

	_, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		HTTPClient:   ts.Client(),
		Subprotocols: []string{ws.BearerSubprotocol, "nope"},
	})
	if err == nil {
		t.Error("a bad token opened a connection")
	}

	conn, resp, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		HTTPClient:   ts.Client(),
		Subprotocols: []string{ws.BearerSubprotocol, "alice-token"},
	})
	if err != nil {
		t.Fatalf("dial with a subprotocol credential: %v", err)
	}
	defer func() {
		if err := conn.CloseNow(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	// The marker comes back, the token never does.
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != ws.BearerSubprotocol {
		t.Errorf("negotiated subprotocol = %q, want %q", got, ws.BearerSubprotocol)
	}
}

// ── a storage backend that is slow and honours its context ───────────────────

const slowBackend = "slowdl"

// slowStore serves results after a delay and aborts when its context is
// canceled, the way the S3 backend does. Without that a download test could not
// tell an exempt route from a timed one.
type slowStore struct {
	mu          sync.Mutex
	objects     map[string][]byte
	sawDeadline bool
}

func registerSlowStore(t *testing.T) *slowStore {
	t.Helper()
	s := &slowStore{objects: make(map[string][]byte)}
	if _, err := storage.GetStore(slowBackend); err != nil {
		storage.Register(slowBackend, s)
		return s
	}
	// storage.Register panics on a duplicate, so the first registration in this
	// binary wins and later tests reuse it.
	existing, _ := storage.GetStore(slowBackend)
	return existing.(*slowStore)
}

func (s *slowStore) Writer(_ context.Context, queryID, format string) (io.WriteCloser, domain.ResultRef, error) {
	ref := domain.ResultRef{Backend: slowBackend, Locator: queryID + "." + format, Format: format}
	return &slowWriter{store: s, locator: ref.Locator}, ref, nil
}

func (s *slowStore) Reader(ctx context.Context, ref domain.ResultRef) (io.ReadCloser, error) {
	_, hasDeadline := ctx.Deadline()

	s.mu.Lock()
	if hasDeadline {
		s.sawDeadline = true
	}
	data, ok := s.objects[ref.Locator]
	s.mu.Unlock()
	if !ok {
		return nil, errors.New("no such object")
	}
	return &slowReader{ctx: ctx, body: bytes.NewReader(data), delay: 500 * time.Millisecond}, nil
}

func (s *slowStore) Stat(_ context.Context, ref domain.ResultRef) (domain.ResultRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref.SizeBytes = int64(len(s.objects[ref.Locator]))
	return ref, nil
}

func (s *slowStore) Delete(_ context.Context, ref domain.ResultRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, ref.Locator)
	return nil
}

func (s *slowStore) readerHadDeadline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawDeadline
}

type slowWriter struct {
	store   *slowStore
	locator string
	buf     bytes.Buffer
}

func (w *slowWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *slowWriter) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.objects[w.locator] = w.buf.Bytes()
	return nil
}

type slowReader struct {
	ctx    context.Context
	body   *bytes.Reader
	delay  time.Duration
	served bool
}

func (r *slowReader) Read(p []byte) (int, error) {
	if !r.served {
		select {
		case <-time.After(r.delay):
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
		r.served = true
	}
	return r.body.Read(p)
}

func (r *slowReader) Close() error { return nil }
