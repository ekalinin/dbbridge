// Package e2e_test contains end-to-end tests that start in-process REST,
// WebSocket and Connect servers backed by an in-memory MetaStore, a local-FS
// ResultStore, and fake database drivers — no external services required.
package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/ratelimit"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/storage/backends/fs"
	"github.com/ekalinin/dbbridge/internal/transport/grpcconnect"
	"github.com/ekalinin/dbbridge/internal/transport/rest"

	"connectrpc.com/connect"
)

// process-wide temp dir for FS result storage — shared across all tests.
var globalResultsDir string

// TestMain registers global singletons once for the whole test binary.
//
// The drivers are registered under real engine names because the config
// validator only accepts those four, and the real drivers are not
// blank-imported here. "slowdb" used to be registered as an engine name of its
// own, which no config could ever reference, so the driver was unreachable.
func TestMain(m *testing.M) {
	db.Register("postgres", fakeDriver{})
	db.Register("mysql", slowDriver{})
	db.Register("clickhouse", failToExecDriver{})

	tmpDir, err := os.MkdirTemp("", "dbbridge-e2e-results-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	globalResultsDir = tmpDir

	fsStore, err := fs.NewFSResultStore(tmpDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create FS store: %v\n", err)
		os.Exit(1)
	}
	storage.Register("fs", fsStore)
	// Registered under the real backend name so a submission can exercise the
	// format check the production ClickHouse store performs, without a container.
	storage.Register("clickhouse", lineStore{fsStore})

	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// harnessOptions parameterizes one in-process server. The zero value is the
// open, unlimited configuration the first e2e tests were written against.
type harnessOptions struct {
	// tokens enables authentication; nil leaves the API open.
	tokens []authn.TokenSpec
	// rps and burst configure the per-caller rate limit; rps <= 0 disables it.
	rps   float64
	burst int
	// allowWrites disables the read-only SQL guard.
	allowWrites bool
	// gcInterval overrides how often garbage collection runs; 0 keeps the
	// one-minute default, which is long enough never to fire during a test.
	gcInterval time.Duration
	// resultTTL is the default result lifetime; 0 means one hour.
	resultTTL time.Duration
}

// testHarness wraps the in-process servers and the wiring behind them. Each
// call gets its own MetaStore and QueryManager so tests are isolated.
type testHarness struct {
	// baseURL serves REST and, under /v1/ws, WebSocket.
	baseURL string
	// grpcURL serves Connect over h2c, as the process does without TLS.
	grpcURL string

	svc *service.QueryService
	qm  *manager.QueryManager
	lm  *lifecycle.Manager
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	return newHarnessWith(t, harnessOptions{})
}

func newHarnessWith(t *testing.T, opts harnessOptions) *testHarness {
	t.Helper()

	if opts.gcInterval == 0 {
		opts.gcInterval = time.Minute
	}
	if opts.resultTTL == 0 {
		opts.resultTTL = time.Hour
	}

	cfgContent := fmt.Sprintf(`
instance:
  id: e2e-test
  metastore: memory
  default_storage: fs
  heartbeat_ttl: 1s
  gc_interval: %s
server:
  rest_addr: ":0"
  grpc_addr: ":0"
defaults:
  result_ttl: %s
  allow_writes: %t
storage:
  fs:
    root: %s
databases:
  - id: testdb
    engine: postgres
    dsn: "postgres://fake:fake@localhost/fake"
    display_name: "Test DB"
    max_conns: 2
  - id: slowdb
    engine: mysql
    dsn: "mysql://fake:fake@localhost/fake"
    display_name: "Slow DB"
    max_conns: 2
  - id: faildb
    engine: clickhouse
    dsn: "clickhouse://fake:fake@localhost/fake"
    display_name: "Failing DB"
    max_conns: 2
`, opts.gcInterval, opts.resultTTL, opts.allowWrites, globalResultsDir)

	cfgFile, err := os.CreateTemp(t.TempDir(), "dbbridge-*.yaml")
	if err != nil {
		t.Fatalf("create config file: %v", err)
	}
	if _, err := cfgFile.WriteString(cfgContent); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	cfgFile.Close()

	cfgMgr, err := config.NewManager(cfgFile.Name())
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}

	metaStore := state.NewMemoryMetaStore()
	t.Cleanup(func() { metaStore.Close() })

	lm := lifecycle.NewManager()

	qm, err := manager.NewQueryManager(cfgMgr, metaStore)
	if err != nil {
		t.Fatalf("manager.NewQueryManager: %v", err)
	}
	t.Cleanup(func() { qm.Close() })

	svc := service.NewQueryService(qm, lm)

	var auth *authn.Authenticator
	if len(opts.tokens) > 0 {
		auth, err = authn.New(opts.tokens)
		if err != nil {
			t.Fatalf("authn.New: %v", err)
		}
	}
	svc.SetAuthRequired(auth != nil)

	limiter := ratelimit.New(opts.rps, opts.burst)

	srv := rest.NewServer(svc, rest.Options{Auth: auth, RateLimit: limiter})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)

	return &testHarness{
		baseURL: hs.URL,
		grpcURL: newConnectServer(t, svc, auth, limiter),
		svc:     svc,
		qm:      qm,
		lm:      lm,
	}
}

// newConnectServer mounts the Connect handler the same way main.go does:
// cleartext HTTP/2, with the rate-limit interceptor ahead of the auth one.
func newConnectServer(t *testing.T, svc *service.QueryService, auth *authn.Authenticator, limiter *ratelimit.Limiter) string {
	t.Helper()

	var interceptors []connect.Interceptor
	if limiter != nil {
		interceptors = append(interceptors, grpcconnect.RateLimitInterceptor(limiter))
	}
	if auth != nil {
		interceptors = append(interceptors, grpcconnect.NewAuthInterceptor(auth))
	}
	var connectOpts []connect.HandlerOption
	if len(interceptors) > 0 {
		connectOpts = append(connectOpts, connect.WithInterceptors(interceptors...))
	}

	mux := http.NewServeMux()
	path, handler := dbbridgev1connect.NewQueryServiceHandler(grpcconnect.NewQueryHandler(svc), connectOpts...)
	mux.Handle(path, handler)

	srv := httptest.NewUnstartedServer(mux)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)

	return srv.URL
}

// connectClient dials the Connect server over h2c.
//
// HTTP1 is deliberately left disabled on this transport. Per
// http.Transport.Protocols: "If Protocols includes UnencryptedHTTP2 and does
// not include HTTP1, the transport will use unencrypted HTTP/2 for requests
// for http:// URLs" - with HTTP1 also enabled, the transport has no
// ALPN-equivalent to negotiate with over plaintext and silently falls back to
// HTTP/1.1, so a client built that way never actually exercises h2c. Do not
// add SetHTTP1(true) back here; the server side (newConnectServer) keeps both
// protocols, matching cmd/dbbridge/main.go, so it still serves plain HTTP/1.1
// callers too - only this test client needs to prove the h2c path works.
func (h *testHarness) connectClient(t *testing.T) dbbridgev1connect.QueryServiceClient {
	t.Helper()
	tr := &http.Transport{}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetUnencryptedHTTP2(true)
	return dbbridgev1connect.NewQueryServiceClient(&http.Client{Transport: tr}, h.grpcURL)
}

// wsURL builds the WebSocket URL for /v1/ws with the given query string.
func (h *testHarness) wsURL(query string) string {
	return strings.Replace(h.baseURL, "http://", "ws://", 1) + "/v1/ws" + query
}

// e2eTokens is the token set the authenticated harnesses share: one subject per
// scope combination the API distinguishes.
func e2eTokens() []authn.TokenSpec {
	return []authn.TokenSpec{
		{Subject: "alice", Value: "alice-token", Scopes: []string{"write"}},
		{Subject: "bob", Value: "bob-token", Scopes: []string{"write"}},
		{Subject: "watcher", Value: "read-token", Scopes: []string{"read"}},
		{Subject: "root", Value: "admin-token", Scopes: []string{"admin"}},
	}
}

// ── Fake drivers ─────────────────────────────────────────────────────────────

// fakeDriver opens pools that return two static rows for any SQL.
// It's used for testing without requiring a real database connection.
type fakeDriver struct{}

// Open creates a new fake database pool.
func (fakeDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return fakePool{}, nil
}

// fakePool implements db.Pool interface for testing purposes.
// It returns predefined static data for any query.
type fakePool struct{}

// Exec executes a query and returns a fake row stream with static data.
// The returned stream contains two rows with columns "id" and "name".
func (fakePool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return &fakeRowStream{
		cols: []string{"id", "name"},
		rows: [][]any{
			{int64(1), "alice"},
			{int64(2), "bob"},
		},
		pos: -1,
	}, nil
}

// Ping simulates a database connectivity check.
func (fakePool) Ping(_ context.Context) error { return nil }

// Stat returns empty pool statistics.
func (fakePool) Stat() db.PoolStat { return db.PoolStat{} }

// Close closes the pool.
func (fakePool) Close() error { return nil }

// fakeRowStream implements db.RowStream for testing purposes.
// It provides a static set of rows for testing query execution.
type fakeRowStream struct {
	cols []string
	rows [][]any
	pos  int
}

// Columns returns the column names of the result set.
func (s *fakeRowStream) Columns() ([]string, error) { return s.cols, nil }

// Next advances to the next row in the result set.
// Returns true if there are more rows, false otherwise.
func (s *fakeRowStream) Next() bool {
	s.pos++
	return s.pos < len(s.rows)
}

// Scan copies the values of the current row into the provided destinations.
func (s *fakeRowStream) Scan(dest ...any) error {
	row := s.rows[s.pos]
	for i, d := range dest {
		if p, ok := d.(*any); ok {
			*p = row[i]
		}
	}
	return nil
}

// Err returns any error that occurred during iteration.
func (s *fakeRowStream) Err() error { return nil }

// Close closes the row stream.
func (s *fakeRowStream) Close() error { return nil }

// slowDriver blocks Exec until its context is canceled (for cancel-query tests).
// It's used to test query cancellation functionality.
type slowDriver struct{}

// Open creates a new slow database pool.
func (slowDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return slowPool{}, nil
}

// slowPool implements db.Pool interface for testing query cancellation.
// Its Exec method blocks until the context is canceled.
type slowPool struct{}

// Exec blocks until the context is canceled, then returns the context error.
// This is used to test query cancellation functionality.
func (slowPool) Exec(ctx context.Context, _ string) (db.RowStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Ping simulates a database connectivity check.
func (slowPool) Ping(_ context.Context) error { return nil }

// Stat returns empty pool statistics.
func (slowPool) Stat() db.PoolStat { return db.PoolStat{} }

// Close closes the pool.
func (slowPool) Close() error { return nil }

// failToExecDriver opens pools whose Exec always fails, so the FAILED path can
// be reached over the public API. Ping succeeds: the failure has to happen
// during execution, not during admission. Named to be visible next to
// internal/testutil.FailToOpenDriver, which is registered under the same
// "clickhouse" engine name in that package's own test binary but fails in
// Open instead - the two never run in the same binary, but a reader jumping
// between the two suites should not assume one behavior from the other.
type failToExecDriver struct{}

func (failToExecDriver) Open(_ context.Context, _ string, _ int) (db.Pool, error) {
	return failToExecPool{}, nil
}

type failToExecPool struct{}

// errExecFailure carries a DSN-shaped secret so a test can assert the API does
// not echo the driver error back to the caller.
var errExecFailure = errors.New("relation \"users\" does not exist (host=db.internal user=admin password=hunter2)")

func (failToExecPool) Exec(_ context.Context, _ string) (db.RowStream, error) {
	return nil, errExecFailure
}
func (failToExecPool) Ping(_ context.Context) error { return nil }
func (failToExecPool) Stat() db.PoolStat            { return db.PoolStat{} }
func (failToExecPool) Close() error                 { return nil }

// lineStore is a stand-in for the ClickHouse ResultStore: it stores bytes
// verbatim on the filesystem but declares the same format contract, so a
// submission that asks for parquet on it is rejected before anything runs.
type lineStore struct {
	storage.ResultStore
}

func (lineStore) SupportsFormat(format string) bool { return format == "jsonl" || format == "csv" }
