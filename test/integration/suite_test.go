//go:build integration

// Package integration exercises dbbridge against the real backends it targets:
// Redis, PostgreSQL, MySQL, S3 (MinIO) and ClickHouse. Everything else in the
// test suite runs on the in-memory MetaStore, the local filesystem and fake
// drivers, so the code paths that only exist for the real backends - Lua
// scripts, lease key expiry, pgx and MySQL type mapping, multipart uploads,
// ClickHouse's line-splitting write path - had no coverage at all.
//
// Run with: make test-containers (requires a Docker daemon).
package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	clickhousestore "github.com/ekalinin/dbbridge/internal/storage/backends/clickhouse"
	"github.com/ekalinin/dbbridge/internal/storage/backends/fs"
	"github.com/ekalinin/dbbridge/internal/storage/backends/s3"
	"github.com/ekalinin/dbbridge/internal/transport/rest"

	_ "github.com/ekalinin/dbbridge/internal/db/drivers/mysql"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/postgres"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// resultsRoot backs the single registered "fs" store: storage.Register panics
// on a duplicate name, so it is set up once for the whole binary.
var resultsRoot string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "dbbridge-integration-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	resultsRoot = dir

	store, err := fs.NewFSResultStore(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	storage.Register("fs", store)

	code := m.Run()

	// The shared "s3" container, if any test used one, is only started via
	// ensureS3Store and is never bound to a single test's cleanup - it has to
	// be torn down here instead.
	if sharedS3C != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sharedS3C.Terminate(ctx)
		cancel()
	}

	// Same reasoning for the shared "clickhouse" store and container, started
	// lazily via ensureClickHouseStore: close the store's connection pool
	// before terminating the server it points at.
	if sharedCHStore != nil {
		if err := sharedCHStore.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "close clickhouse store:", err)
		}
	}
	if sharedCHC != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sharedCHC.Terminate(ctx)
		cancel()
	}

	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// terminate stops a container at the end of a test rather than leaking it into
// the next run.
func terminate(t *testing.T, c testcontainers.Container) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.Terminate(ctx); err != nil {
			t.Errorf("terminate container: %v", err)
		}
	})
}

// startRedis brings up Redis and returns its host:port.
func startRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	terminate(t, c)

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("redis host: %v", err)
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("redis port: %v", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port())
}

// startPostgres brings up PostgreSQL with a seeded table and returns a DSN.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("dbbridge"),
		tcpostgres.WithUsername("dbbridge"),
		tcpostgres.WithPassword("dbbridge"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	terminate(t, c)

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	// Seeded through the project's own driver, so the test depends on nothing
	// beyond what dbbridge itself needs.
	seed(t, "postgres", dsn,
		"CREATE TABLE users (id int, name text)",
		"INSERT INTO users VALUES (1, 'alice'), (2, 'bob')",
	)
	return dsn
}

// startMySQL brings up MySQL with the same seeded table and returns a DSN.
func startMySQL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := tcmysql.Run(ctx, "mysql:8.4",
		tcmysql.WithDatabase("dbbridge"),
		tcmysql.WithUsername("dbbridge"),
		tcmysql.WithPassword("dbbridge"),
		// The module default is 60s, and a cold MySQL 8.4 initialization on a
		// loaded CI runner takes longer than that often enough to matter.
		testcontainers.WithWaitStrategy(
			wait.ForLog("port: 3306  MySQL Community Server").
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	terminate(t, c)

	dsn, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}

	seed(t, "mysql", dsn,
		"CREATE TABLE users (id int, name varchar(32))",
		"INSERT INTO users VALUES (1, 'alice'), (2, 'bob')",
	)
	return dsn
}

// seed runs DDL and DML through the registered driver.
func seed(t *testing.T, engine, dsn string, statements ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	pool, err := db.OpenPool(ctx, engine, dsn, 2)
	if err != nil {
		t.Fatalf("open %s pool: %v", engine, err)
	}
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close %s pool: %v", engine, err)
		}
	}()

	for _, stmt := range statements {
		rows, err := pool.Exec(ctx, stmt)
		if err != nil {
			t.Fatalf("seed %s with %q: %v", engine, stmt, err)
		}
		for rows.Next() {
		}
		// pgx reports a good part of its server-side errors here rather than
		// from Exec, so without this a failed CREATE TABLE passed silently and
		// surfaced much later as "rows_read = 0, want 2".
		if err := rows.Err(); err != nil {
			t.Fatalf("seed %s with %q: %v", engine, stmt, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close seed rows: %v", err)
		}
	}
}

// minioEndpoint describes a running MinIO with a bucket ready to use.
type minioEndpoint struct {
	endpoint string
	keyID    string
	secret   string
	bucket   string
}

// newMinIOContainer starts a MinIO container with a bucket ready to use and
// hands the container back rather than tying its lifetime to a *testing.T, so
// a caller that needs it to outlive a single test - ensureS3Store below - can
// decide when it gets torn down.
func newMinIOContainer(ctx context.Context) (minioEndpoint, testcontainers.Container, error) {
	c, err := tcminio.Run(ctx, "minio/minio:RELEASE.2025-09-07T16-13-09Z")
	if err != nil {
		return minioEndpoint{}, nil, err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return minioEndpoint{}, c, err
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return minioEndpoint{}, c, err
	}

	ep := minioEndpoint{
		endpoint: fmt.Sprintf("http://%s:%s", host, port.Port()),
		keyID:    c.Username,
		secret:   c.Password,
		bucket:   "dbbridge",
	}

	// The bucket is created with the SDK rather than the mc client, so the test
	// does not depend on which tools the image happens to ship.
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ep.keyID, ep.secret, "")),
	)
	if err != nil {
		return minioEndpoint{}, c, err
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(ep.endpoint)
		o.UsePathStyle = true
	})
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(ep.bucket)}); err != nil {
		return minioEndpoint{}, c, err
	}

	return ep, c, nil
}

// sharedS3 holds the single MinIO container that backs the "s3" storage
// backend for the whole test binary.
var (
	sharedS3Once sync.Once
	sharedS3Ep   minioEndpoint
	sharedS3C    testcontainers.Container
	sharedS3Err  error
)

// ensureS3Store lazily brings up one MinIO container and registers it as the
// "s3" storage backend, then returns its endpoint. storage.Register panics on
// a second registration under the same name and there is no way to
// unregister, so every test that needs the "s3" backend has to share this one
// registration. The container this returns is not torn down at the end of
// whichever test happens to trigger it - a later test may still need it - so
// TestMain terminates it once the whole binary is done.
func ensureS3Store(t *testing.T) minioEndpoint {
	t.Helper()
	sharedS3Once.Do(func() {
		ep, c, err := newMinIOContainer(context.Background())
		// Record whatever container came back, even on failure, before checking
		// err: newMinIOContainer can fail after the container is already up (a
		// bucket create failure, say), and sharedS3C is what lets TestMain tear
		// it down instead of leaking it for the rest of the run.
		sharedS3C = c
		if err != nil {
			sharedS3Err = fmt.Errorf("start shared minio: %w", err)
			return
		}
		sharedS3Ep = ep

		store, err := s3.NewS3ResultStore(context.Background(), ep.bucket, "us-east-1", ep.endpoint, ep.keyID, ep.secret)
		if err != nil {
			sharedS3Err = fmt.Errorf("NewS3ResultStore: %w", err)
			return
		}
		storage.Register("s3", store)
	})
	// sync.Once.Do still marks itself done when f calls t.Fatalf (runtime.Goexit
	// runs the deferred done.Store(1) on the way out), so a failure inside the
	// closure above must not itself call t.Fatalf: every caller, not just the
	// first, has to see it and fail loudly rather than get a zero-value
	// endpoint silently.
	if sharedS3Err != nil {
		t.Fatalf("shared minio unavailable: %v", sharedS3Err)
	}
	return sharedS3Ep
}

// chTable is the results table the shared ClickHouse store is pointed at.
const chTable = "dbbridge_results"

// newClickHouseContainer starts a ClickHouse container and returns a DSN for
// it, without tying its lifetime to a *testing.T - the same split as
// newMinIOContainer above, for the same reason: ensureClickHouseStore needs
// the container to outlive whichever single test happens to start it.
func newClickHouseContainer(ctx context.Context) (string, testcontainers.Container, error) {
	c, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:24.8-alpine",
		tcclickhouse.WithUsername("dbbridge"),
		tcclickhouse.WithPassword("dbbridge"),
		tcclickhouse.WithDatabase("dbbridge"),
		// The module's own default wait strategy is an HTTP health check on
		// 8123, which is what actually gates readiness; replacing it with a
		// plain "is 9000 listening" check let the native protocol port accept
		// TCP connections before the server could complete a handshake on it,
		// and NewClickHouseResultStore failed with "read: EOF" while creating
		// the results table. Keep the same HTTP check, just with more time
		// for a cold image pull.
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("8123/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		return "", nil, err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", c, err
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return "", c, err
	}

	return fmt.Sprintf("clickhouse://dbbridge:dbbridge@%s:%s/dbbridge", host, port.Port()), c, nil
}

// sharedClickHouse holds the single ClickHouse container, DSN and store that
// back the "clickhouse" storage backend for the whole test binary.
var (
	sharedCHOnce  sync.Once
	sharedCHDSN   string
	sharedCHC     testcontainers.Container
	sharedCHStore *clickhousestore.ClickHouseResultStore
	sharedCHErr   error
)

// ensureClickHouseStore lazily brings up one ClickHouse container and
// registers it as the "clickhouse" storage backend, then returns its DSN.
// ClickHouse backs the ResultStore, not a query target: it is the only
// storage backend with no live coverage, and its line-splitting write path
// cannot be checked in-memory. It follows the same shared-registration shape
// as ensureS3Store above, rather than the container living inside a single
// test: storage.Register panics on a second registration under the same
// name and there is no way to unregister, so every test that needs the
// "clickhouse" backend has to share this one registration. The container is
// not torn down at the end of whichever test happens to trigger it - a later
// test may still need it - so TestMain terminates it once the whole binary
// is done.
func ensureClickHouseStore(t *testing.T) string {
	t.Helper()
	sharedCHOnce.Do(func() {
		dsn, c, err := newClickHouseContainer(context.Background())
		// Record whatever container came back, even on failure, before checking
		// err - see the matching comment in ensureS3Store.
		sharedCHC = c
		if err != nil {
			sharedCHErr = fmt.Errorf("start shared clickhouse: %w", err)
			return
		}
		sharedCHDSN = dsn

		store, err := clickhousestore.NewClickHouseResultStore(dsn, chTable)
		if err != nil {
			sharedCHErr = fmt.Errorf("NewClickHouseResultStore: %w", err)
			return
		}
		sharedCHStore = store
		storage.Register("clickhouse", store)
	})
	// See ensureS3Store: sync.Once.Do marks itself done even on a t.Fatalf
	// inside f, so the failure has to be surfaced to every caller here instead.
	if sharedCHErr != nil {
		t.Fatalf("shared clickhouse unavailable: %v", sharedCHErr)
	}
	return sharedCHDSN
}

// harness wires a full service against the given backends.
type harness struct {
	svc *service.QueryService
	qm  *manager.QueryManager
	ms  state.MetaStore
}

type harnessOptions struct {
	instanceID     string
	redisAddr      string
	databases      string
	defaultStorage string
	storageSection string
	metaStore      state.MetaStore
	// gcInterval overrides how often garbage collection runs; 0 keeps the
	// one-minute default, which is long enough never to fire during a test.
	gcInterval time.Duration
}

func newHarness(t *testing.T, opts harnessOptions) *harness {
	t.Helper()

	if opts.gcInterval == 0 {
		opts.gcInterval = time.Minute
	}
	if opts.defaultStorage == "" {
		opts.defaultStorage = "fs"
	}
	if opts.storageSection == "" {
		opts.storageSection = fmt.Sprintf("  fs:\n    root: %s\n", resultsRoot)
	}

	cfgBody := fmt.Sprintf(`
instance:
  id: %s
  metastore: redis
  redis_addr: %q
  default_storage: %s
  heartbeat_ttl: 1s
  gc_interval: %s
server:
  rest_addr: ":0"
  grpc_addr: ":0"
defaults:
  result_ttl: 1h
storage:
%s
databases:
%s
`, opts.instanceID, opts.redisAddr, opts.defaultStorage, opts.gcInterval, opts.storageSection, opts.databases)

	path := filepath.Join(t.TempDir(), "dbbridge.yaml")
	if err := os.WriteFile(path, []byte(cfgBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfgMgr, err := config.NewManager(path)
	if err != nil {
		t.Fatalf("config.NewManager: %v", err)
	}

	ms := opts.metaStore
	if ms == nil {
		redisStore := state.NewRedisMetaStore(opts.redisAddr, "", 0)
		t.Cleanup(func() {
			if err := redisStore.Close(); err != nil {
				t.Errorf("close metastore: %v", err)
			}
		})
		ms = redisStore
	}

	qm, err := manager.NewQueryManager(cfgMgr, ms)
	if err != nil {
		t.Fatalf("manager.NewQueryManager: %v", err)
	}
	t.Cleanup(func() {
		if err := qm.Close(); err != nil {
			t.Errorf("close query manager: %v", err)
		}
	})

	return &harness{
		svc: service.NewQueryService(qm, lifecycle.NewManager()),
		qm:  qm,
		ms:  ms,
	}
}

// newRESTServer puts the REST transport in front of a harness, so a test can
// drive the API the way a client does rather than calling the service directly.
func newRESTServer(t *testing.T, h *harness) string {
	t.Helper()
	srv := httptest.NewServer(rest.NewServer(h.svc, rest.Options{}).Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

// pgDatabases is the databases section for a PostgreSQL target.
func pgDatabases(dsn string) string {
	return fmt.Sprintf("  - id: pg\n    engine: postgres\n    dsn: %q\n    max_conns: 4\n", dsn)
}

// mysqlDatabases is the databases section for a MySQL target.
func mysqlDatabases(dsn string) string {
	return fmt.Sprintf("  - id: my\n    engine: mysql\n    dsn: %q\n    max_conns: 4\n", dsn)
}
