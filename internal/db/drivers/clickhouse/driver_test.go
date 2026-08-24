package clickhouse

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/testutil/dbtest"
)

// malformedDSN has no host to resolve, so the ping clickhouse-go performs on
// the way out of Open fails on the DSN itself - no server, and no timeout to
// wait out.
const malformedDSN = "not-a-clickhouse-dsn"

// TestDriverIsRegistered: cmd/dbbridge/main.go wires this engine in with a
// blank import and nothing else, so the init above is the whole contract
// between the config's `engine: clickhouse` and this package.
func TestDriverIsRegistered(t *testing.T) {
	_, err := db.OpenPool(context.Background(), "clickhouse", malformedDSN, 1)
	if err == nil {
		t.Fatal("OpenPool with a malformed DSN returned no error")
	}
	if strings.Contains(err.Error(), "unknown database engine") {
		t.Fatalf(`nothing is registered under "clickhouse": %v`, err)
	}
}

// TestOpenRejectsMalformedDSN keeps a bad DSN a startup error rather than a
// pool that only fails once a query is running.
func TestOpenRejectsMalformedDSN(t *testing.T) {
	pool, err := (&clickhouseDriver{}).Open(context.Background(), malformedDSN, 4)
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

// TestPool covers the wrappers around database/sql. Open itself pings, so a
// pool cannot be built through it without a server; the driver's own pool type
// over a fake handle is as close as a Docker-free test gets. The engine is
// exercised for real in test/integration.
func TestPool(t *testing.T) {
	dbtest.CheckSQLPool(t, func(handle *sql.DB) db.Pool { return &clickhousePool{db: handle} })
}
