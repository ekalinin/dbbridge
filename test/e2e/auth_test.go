package e2e_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/transport/ws"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
)

// doAuth issues a request with an optional bearer token.
func doAuth(t *testing.T, method, url, token, body string) *http.Response {
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

const submitBody = `{"database_id":"testdb","sql":"SELECT 1","options":{"mode":"sync"}}`

func TestAuth_ScopeMatrix(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens()})

	cases := []struct {
		name   string
		method string
		path   string
		token  string
		body   string
		want   int
	}{
		{"no token is unauthorized", http.MethodGet, "/v1/databases", "", "", http.StatusUnauthorized},
		{"bad token is unauthorized", http.MethodGet, "/v1/databases", "nope", "", http.StatusUnauthorized},
		{"read token lists databases", http.MethodGet, "/v1/databases", "read-token", "", http.StatusOK},
		{"read token cannot submit", http.MethodPost, "/v1/queries", "read-token", submitBody, http.StatusForbidden},
		{"write token submits", http.MethodPost, "/v1/queries", "alice-token", submitBody, http.StatusOK},
		{"write token implies read", http.MethodGet, "/v1/databases", "alice-token", "", http.StatusOK},
		{"read token cannot reload", http.MethodPost, "/v1/admin/reload", "read-token", "", http.StatusForbidden},
		{"write token cannot reload", http.MethodPost, "/v1/admin/reload", "alice-token", "", http.StatusForbidden},
		{"admin reloads", http.MethodPost, "/v1/admin/reload", "admin-token", "", http.StatusOK},
		{"read token cannot scrape metrics", http.MethodGet, "/metrics", "read-token", "", http.StatusForbidden},
		{"admin scrapes metrics", http.MethodGet, "/metrics", "admin-token", "", http.StatusOK},
		{"probes stay open", http.MethodGet, "/healthz", "", "", http.StatusOK},
		{"readiness stays open", http.MethodGet, "/readyz", "", "", http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doAuth(t, tc.method, h.baseURL+tc.path, tc.token, tc.body)
			defer resp.Body.Close()
			assertStatus(t, resp, tc.want)
		})
	}
}

// TestAuth_UnauthorizedCarriesTheEnvelope: a 401 has to be parseable by the
// same client code as every other error, and has to advertise the scheme.
func TestAuth_UnauthorizedCarriesTheEnvelope(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens()})

	resp := doAuth(t, http.MethodGet, h.baseURL+"/v1/databases", "", "")
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusUnauthorized)

	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", got)
	}
	if got := resp.Header.Get("Vary"); !strings.Contains(got, "Authorization") {
		t.Errorf("Vary = %q, want it to include Authorization", got)
	}

	var body struct {
		Error     string `json:"error"`
		RequestID string `json:"request_id"`
	}
	decodeJSON(t, resp.Body, &body)
	if body.Error == "" {
		t.Error("401 body has no error field")
	}
	if body.RequestID == "" {
		t.Error("401 body has no request_id")
	}
}

// TestAuth_QueriesAreBoundToTheirSubject: knowing a query ID must not be enough
// to read someone else's SQL, stats or result. A foreign record reads as 404,
// not 403, so the API does not confirm the ID exists.
func TestAuth_QueriesAreBoundToTheirSubject(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens()})

	resp := doAuth(t, http.MethodPost, h.baseURL+"/v1/queries", "alice-token", submitBody)
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)
	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)

	for _, path := range []string{"", "/stats", "/result"} {
		url := h.baseURL + "/v1/queries/" + rec.ID + path
		mine := doAuth(t, http.MethodGet, url, "alice-token", "")
		assertStatus(t, mine, http.StatusOK)
		mine.Body.Close()

		theirs := doAuth(t, http.MethodGet, url, "bob-token", "")
		assertStatus(t, theirs, http.StatusNotFound)
		theirs.Body.Close()
	}

	// Admin acts across subjects.
	asRoot := doAuth(t, http.MethodGet, h.baseURL+"/v1/queries/"+rec.ID, "admin-token", "")
	defer asRoot.Body.Close()
	assertStatus(t, asRoot, http.StatusOK)

	// Stopping is a write on someone else's resource and reads as 404 too.
	stop := doAuth(t, http.MethodPost, h.baseURL+"/v1/queries/"+rec.ID+":stop", "bob-token", "")
	defer stop.Body.Close()
	assertStatus(t, stop, http.StatusNotFound)
}

// TestAuth_WebSocketTakesASubprotocolCredential: a browser cannot set the
// Authorization header on a handshake, so the token rides in the subprotocol
// list. Only the marker comes back, never the token.
func TestAuth_WebSocketTakesASubprotocolCredential(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens()})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := websocket.Dial(ctx, h.wsURL(""), nil); err == nil {
		t.Fatal("WebSocket handshake succeeded without a credential")
	}

	conn, resp, err := websocket.Dial(ctx, h.wsURL(""), &websocket.DialOptions{
		Subprotocols: []string{ws.BearerSubprotocol, "read-token"},
	})
	if err != nil {
		t.Fatalf("ws dial with a subprotocol credential: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != ws.BearerSubprotocol {
		t.Errorf("negotiated subprotocol = %q, want only the marker %q", got, ws.BearerSubprotocol)
	}
}

// TestAuth_ConnectEnforcesTheSameScopes mirrors the REST matrix over Connect,
// where the gate is an interceptor rather than chi middleware.
func TestAuth_ConnectEnforcesTheSameScopes(t *testing.T) {
	h := newHarnessWith(t, harnessOptions{tokens: e2eTokens()})
	c := h.connectClient(t)

	call := func(token string, fn func(context.Context, string) error) error {
		return fn(context.Background(), token)
	}

	start := func(ctx context.Context, token string) error {
		req := connect.NewRequest(&v1.StartQueryRequest{
			DatabaseId: "testdb",
			Sql:        "SELECT 1",
			Options:    &v1.QueryOptions{Mode: "sync"},
		})
		if token != "" {
			req.Header().Set("Authorization", "Bearer "+token)
		}
		_, err := c.StartQuery(ctx, req)
		return err
	}
	reload := func(ctx context.Context, token string) error {
		req := connect.NewRequest(&v1.ReloadConfigRequest{})
		if token != "" {
			req.Header().Set("Authorization", "Bearer "+token)
		}
		_, err := c.ReloadConfig(ctx, req)
		return err
	}

	if got := connect.CodeOf(call("", start)); got != connect.CodeUnauthenticated {
		t.Errorf("StartQuery without a token = %v, want unauthenticated", got)
	}
	if got := connect.CodeOf(call("read-token", start)); got != connect.CodePermissionDenied {
		t.Errorf("StartQuery with a read token = %v, want permission denied", got)
	}
	if err := call("alice-token", start); err != nil {
		t.Errorf("StartQuery with a write token: %v", err)
	}
	if got := connect.CodeOf(call("alice-token", reload)); got != connect.CodePermissionDenied {
		t.Errorf("ReloadConfig with a write token = %v, want permission denied", got)
	}
	if err := call("admin-token", reload); err != nil {
		t.Errorf("ReloadConfig with an admin token: %v", err)
	}
}
