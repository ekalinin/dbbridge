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

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/rest"
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
		{"bad format", `{"database_id":"testdb","sql":"SELECT 1","options":{"result_format":"parquet"}}`, http.StatusBadRequest},
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
