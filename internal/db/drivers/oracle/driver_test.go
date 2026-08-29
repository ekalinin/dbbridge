package oracle

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/testutil/dbtest"
)

// malformedDSN carries no port, so the ping go-ora performs on the way out of
// Open fails on the DSN itself - no server, and no timeout to wait out.
const malformedDSN = "not-an-oracle-dsn"

// TestDriverIsRegistered: cmd/dbbridge/main.go wires this engine in with a
// blank import and nothing else, so the init above is the whole contract
// between the config's `engine: oracle` and this package.
func TestDriverIsRegistered(t *testing.T) {
	_, err := db.OpenPool(context.Background(), "oracle", malformedDSN, 1)
	if err == nil {
		t.Fatal("OpenPool with a malformed DSN returned no error")
	}
	if strings.Contains(err.Error(), "unknown database engine") {
		t.Fatalf(`nothing is registered under "oracle": %v`, err)
	}
}

// TestOpenRejectsMalformedDSN keeps a bad DSN a startup error rather than a
// pool that only fails once a query is running.
func TestOpenRejectsMalformedDSN(t *testing.T) {
	pool, err := (&oracleDriver{}).Open(context.Background(), malformedDSN, 4)
	if err == nil {
		if cerr := pool.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
		t.Fatal("Open with a malformed DSN returned a pool")
	}
	if pool != nil {
		t.Errorf("Open returned a pool alongside the error %v", err)
	}
}

// TestPool covers the wrappers around database/sql without a server: Open
// pings, so a pool cannot be built through it here, and the pool type is
// checked over a fake handle instead. The same wrappers run against a real
// Oracle in test/integration's TestOracle, which is opt-in - see `make
// test-containers-oracle`.
func TestPool(t *testing.T) {
	dbtest.CheckSQLPool(t, func(handle *sql.DB) db.Pool { return &oraclePool{db: handle} })
}
