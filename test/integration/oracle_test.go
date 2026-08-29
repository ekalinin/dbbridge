//go:build integration && oracle

// Oracle is the one advertised engine whose query path had never been run
// (#29). It sits behind a second build tag rather than next to PostgreSQL and
// MySQL in suite_test.go because of what the container costs: there is no
// testcontainers module for Oracle, so it is a generic request, and
// gvenzl/oracle-free unpacks to about 6 GB, which every pull request would
// otherwise pay for. Run it with `make test-containers-oracle`, or leave it to
// the weekly Oracle workflow - which also fires on any pull request touching
// the driver or this file.
//
// Everything Oracle lives here, blank import included, so that `make
// test-containers` builds a binary with no trace of it.

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/db"

	_ "github.com/ekalinin/dbbridge/internal/db/drivers/oracle"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// oracleImage: the "faststart" variant ships a database that is already
	// created, which turns a multi-minute first boot into seconds. It is
	// published for both amd64 and arm64, so the same tag serves CI and an
	// Apple Silicon workstation.
	oracleImage = "gvenzl/oracle-free:23-slim-faststart"

	// oracleService is the pluggable database the image creates APP_USER in.
	// The listener also serves the container database as "FREE", which has no
	// such user - connecting there fails authentication rather than timing out,
	// so the distinction is worth naming.
	oracleService = "FREEPDB1"
)

// startOracle brings up Oracle with the same seeded table the other engines
// use and returns a DSN. It is the counterpart of startPostgres and startMySQL
// in suite_test.go, written against testcontainers.Run directly because no
// module wraps this engine.
func startOracle(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.Run(ctx, oracleImage,
		testcontainers.WithExposedPorts("1521/tcp"),
		testcontainers.WithEnv(map[string]string{
			// ORACLE_PASSWORD is SYS and SYSTEM, which nothing here uses; the
			// image refuses to start without it.
			"ORACLE_PASSWORD":   "dbbridge",
			"APP_USER":          "dbbridge",
			"APP_USER_PASSWORD": "dbbridge",
		}),
		// The image prints this line once the listener is serving the PDB.
		// Waiting on the port instead would return while the instance is still
		// mounting, and the ping go-ora performs inside Open would fail.
		testcontainers.WithWaitStrategy(
			wait.ForLog("DATABASE IS READY TO USE!").
				WithStartupTimeout(5*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("start oracle: %v", err)
	}
	terminate(t, c)

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("oracle host: %v", err)
	}
	port, err := c.MappedPort(ctx, "1521/tcp")
	if err != nil {
		t.Fatalf("oracle port: %v", err)
	}
	dsn := fmt.Sprintf("oracle://dbbridge:dbbridge@%s:%s/%s", host, port.Port(), oracleService)

	seedOracle(t, dsn,
		"CREATE TABLE users (id NUMBER(10), name VARCHAR2(32))",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	)
	return dsn
}

// seedOracle runs DDL and DML over database/sql rather than through db.Pool the
// way seed() does, for the reason seedClickHouse has the same shape: Pool.Exec
// is QueryContext, and a statement that returns no rows belongs on ExecContext.
// The statements themselves are Oracle's dialect - it has no multi-row VALUES,
// so a row is an INSERT of its own, and a trailing semicolon is a syntax error
// rather than a separator.
func seedOracle(t *testing.T, dsn string, statements ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatalf("open oracle for seeding: %v", err)
	}
	defer conn.Close()

	for _, stmt := range statements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed oracle with %q: %v", stmt, err)
		}
	}
}

// oracleDatabases is the databases section for an Oracle target.
func oracleDatabases(dsn string) string {
	return fmt.Sprintf("  - id: ora\n    engine: oracle\n    dsn: %q\n    max_conns: 4\n", dsn)
}

// TestOracle is the whole Oracle group. One container serves both subtests:
// starting it is by far the most expensive thing in this suite, and startOracle
// ties it to this test rather than to the binary.
func TestOracle(t *testing.T) {
	dsn := startOracle(t)

	t.Run("EndToEnd", func(t *testing.T) { testOracleEndToEnd(t, dsn) })
	t.Run("PoolStat", func(t *testing.T) { testOraclePoolStat(t, dsn) })
}

// testOracleEndToEnd is TestPostgres_EndToEnd against Oracle: a query run
// through a real Redis MetaStore, materialized to the filesystem, and read back.
func testOracleEndToEnd(t *testing.T, dsn string) {
	redisAddr := startRedis(t)

	h := newHarness(t, harnessOptions{
		instanceID: "it-ora",
		redisAddr:  redisAddr,
		databases:  oracleDatabases(dsn),
	})

	rec, err := h.svc.StartQuery(context.Background(), "ora", "SELECT id, name FROM users ORDER BY id",
		domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	final := pollTerminal(t, h, rec.ID, 60*time.Second)
	if final.State != domain.StateSucceeded {
		t.Fatalf("state = %s, error = %+v", final.State, final.Error)
	}
	if final.Stats.RowsRead != 2 {
		t.Errorf("rows_read = %d, want 2", final.Stats.RowsRead)
	}
	if final.Result == nil || !strings.HasPrefix(final.Result.Checksum, "sha256:") {
		t.Errorf("result ref = %+v, want a sha256 checksum", final.Result)
	}

	// Oracle folds unquoted identifiers to upper case, so what RowStream.Columns
	// reports - and therefore what the JSONL keys are - is ID and NAME, not the
	// id and name the statement was written with. That is what an Oracle user
	// actually gets back, so it is asserted rather than hidden behind a quoted
	// alias.
	rows := readResult(t, h, rec.ID)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want 2", rows)
	}
	for i, want := range []string{"alice", "bob"} {
		if _, ok := rows[i]["ID"]; !ok {
			t.Errorf("row %d = %+v, want an ID column", i, rows[i])
		}
		if rows[i]["NAME"] != want {
			t.Errorf("row %d NAME = %v, want %s", i, rows[i]["NAME"], want)
		}
	}
}

// testOraclePoolStat is TestPostgres_PoolStat for this driver. The mapping of
// database/sql's counters onto db.PoolStat is what /metrics and the admin
// can-stop check read, and the driver's own unit test can only watch it through
// a handle that was never dialed, where every counter reads zero and a swapped
// Idle and InUse looks exactly like a correct one.
func testOraclePoolStat(t *testing.T, dsn string) {
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, "oracle", dsn, 4)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close pool: %v", err)
		}
	}()

	// Open pings on the way out, which is what puts the first connection in the
	// pool; it hands it straight back, so it ends up idle.
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	assertPoolStat(t, pool.Stat(), "after a ping", 1, 0)

	// A row stream holds its connection until it is closed.
	rows, err := pool.Exec(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	assertPoolStat(t, pool.Stat(), "while a result set is open", 0, 1)

	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}
	assertPoolStat(t, pool.Stat(), "after the result set was closed", 1, 0)
}
