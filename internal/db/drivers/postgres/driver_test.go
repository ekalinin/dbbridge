package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

// idleDSN parses cleanly and is never dialed: pgxpool connects lazily, so a
// pool over it can be opened, inspected and closed without a server. Port 1 is
// there to make a stray dial fail immediately rather than hang.
const idleDSN = "postgres://dbbridge:dbbridge@127.0.0.1:1/dbbridge?sslmode=disable"

const malformedDSN = "not a postgres dsn"

// TestDriverIsRegistered: cmd/dbbridge/main.go wires this engine in with a
// blank import and nothing else, so the init above is the whole contract
// between the config's `engine: postgres` and this package.
func TestDriverIsRegistered(t *testing.T) {
	pool, err := db.OpenPool(context.Background(), "postgres", idleDSN, 4)
	if err != nil {
		t.Fatalf(`OpenPool("postgres"): %v`, err)
	}
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if _, ok := pool.(*postgresPool); !ok {
		t.Errorf("OpenPool returned %T, want this package's *postgresPool", pool)
	}
}

// TestOpenRejectsMalformedDSN keeps a bad DSN a startup error rather than a
// pool that only fails once a query is running.
func TestOpenRejectsMalformedDSN(t *testing.T) {
	pool, err := (&postgresDriver{}).Open(context.Background(), malformedDSN, 4)
	if err == nil {
		if cerr := pool.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
		t.Fatal("Open with a malformed DSN returned a pool")
	}
	if pool != nil {
		t.Errorf("Open returned a pool alongside the error %v", err)
	}
	if !strings.Contains(err.Error(), malformedDSN) {
		t.Errorf("error = %q, want the rejected DSN in it", err)
	}
}

// TestOpenAppliesMaxConns: max_conns is the config's only bound on what a
// single database can hold open against a target, and it reaches pgx through
// a field on the parsed config rather than a setter on the pool.
func TestOpenAppliesMaxConns(t *testing.T) {
	pool := openIdlePool(t, 7)
	if got := pool.pool.Config().MaxConns; got != 7 {
		t.Errorf("MaxConns = %d, want 7", got)
	}
}

// TestOpenKeepsThePgxDefaultWithoutMaxConns: max_conns is optional in the
// config, and zero must not become a pool that can hold no connections.
func TestOpenKeepsThePgxDefaultWithoutMaxConns(t *testing.T) {
	parsed, err := pgxpool.ParseConfig(idleDSN)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}

	pool := openIdlePool(t, 0)
	if got := pool.pool.Config().MaxConns; got != parsed.MaxConns {
		t.Errorf("MaxConns = %d, want the pgx default %d", got, parsed.MaxConns)
	}
}

// TestStatOnAnIdlePool: with no connection ever acquired every counter reads
// zero, which is also what a caller polling metrics sees before the first
// query. The mapping onto non-zero counters needs a live server and belongs to
// test/integration.
func TestStatOnAnIdlePool(t *testing.T) {
	pool := openIdlePool(t, 4)
	if got := pool.Stat(); got != (db.PoolStat{}) {
		t.Errorf("Stat on an idle pool = %+v, want the zero value", got)
	}
}

// TestCloseIsIdempotent: Reload closes pools that dropped out of the config
// while shutdown closes whatever is left, and the two can meet on one pool.
func TestCloseIsIdempotent(t *testing.T) {
	pool := openIdlePool(t, 4)
	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

// openIdlePool opens a pool that is never dialed and closes it at the end of
// the test.
func openIdlePool(t *testing.T, maxConns int) *postgresPool {
	t.Helper()

	pool, err := (&postgresDriver{}).Open(context.Background(), idleDSN, maxConns)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		// Idempotent, so a test that closes the pool itself is fine.
		if err := pool.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	pg, ok := pool.(*postgresPool)
	if !ok {
		t.Fatalf("Open returned %T, want *postgresPool", pool)
	}
	return pg
}
