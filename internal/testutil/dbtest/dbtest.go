// Package dbtest provides a network-free database/sql driver and the shared
// db.Pool contract check for the drivers built on top of database/sql -
// mysql, clickhouse and oracle. Their pool and row-stream wrappers are the
// same code three times over, and every one of them needs a live server to
// reach through Open, so `make test-unit` cannot get at them that way: the
// check below builds each driver's own pool type over a fake *sql.DB instead.
package dbtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/ekalinin/dbbridge/internal/db"
)

// driverName is the name the fake is registered under with database/sql. It
// deliberately does not collide with a real engine name: the driver packages
// register theirs in the same test binary.
const driverName = "dbbridge-fake"

// failingQuery makes the fake fail mid-iteration, so a caller can check that
// the error surfaces from Err() after Next() has reported false.
const failingQuery = "SELECT boom"

var (
	fakeColumns = []string{"id", "name"}
	fakeRows    = [][]driver.Value{{int64(1), "alice"}, {int64(2), "bob"}}
	errBoom     = errors.New("dbtest: the fake result set broke")
)

func init() { sql.Register(driverName, fakeDriver{}) }

// OpenFakeDB returns a *sql.DB whose connections open without touching the
// network and whose queries return two canned rows.
func OpenFakeDB(t *testing.T) *sql.DB {
	t.Helper()
	handle, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open %s: %v", driverName, err)
	}
	t.Cleanup(func() {
		// Idempotent, so a test that closes the pool itself is fine.
		if err := handle.Close(); err != nil {
			t.Errorf("close fake handle: %v", err)
		}
	})
	return handle
}

// CheckSQLPool exercises the parts of the db.Pool and db.RowStream contract
// that a database/sql-backed driver gets from its wrappers. newPool wraps the
// given handle in the driver's own pool type; it is called once per check, so
// each one starts from an untouched pool.
func CheckSQLPool(t *testing.T, newPool func(*sql.DB) db.Pool) {
	t.Helper()

	t.Run("Stat", func(t *testing.T) {
		handle := OpenFakeDB(t)
		pool := newPool(handle)

		if got := pool.Stat(); got != (db.PoolStat{}) {
			t.Fatalf("Stat on an unused pool = %+v, want the zero value", got)
		}

		// A checked-out connection separates the three counters, which all
		// read the same zero until one of them is held.
		conn, err := handle.Conn(context.Background())
		if err != nil {
			t.Fatalf("check out a connection: %v", err)
		}
		if got, want := pool.Stat(), (db.PoolStat{Open: 1, Idle: 0, InUse: 1}); got != want {
			t.Errorf("Stat while a connection is held = %+v, want %+v", got, want)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("release the connection: %v", err)
		}
		if got, want := pool.Stat(), (db.PoolStat{Open: 1, Idle: 1, InUse: 0}); got != want {
			t.Errorf("Stat after the connection went back = %+v, want %+v", got, want)
		}
	})

	t.Run("Ping", func(t *testing.T) {
		pool := newPool(OpenFakeDB(t))
		if err := pool.Ping(context.Background()); err != nil {
			t.Errorf("Ping: %v", err)
		}
	})

	t.Run("Exec streams rows", func(t *testing.T) {
		pool := newPool(OpenFakeDB(t))

		rows, err := pool.Exec(context.Background(), "SELECT id, name FROM t")
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}

		// Before the first Next: the encoder writes the header out of
		// Columns() while the result set is still untouched.
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Columns: %v", err)
		}
		if len(cols) != len(fakeColumns) || cols[0] != fakeColumns[0] || cols[1] != fakeColumns[1] {
			t.Errorf("Columns = %v, want %v", cols, fakeColumns)
		}

		var got [][]any
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			got = append(got, []any{id, name})
		}
		if err := rows.Err(); err != nil {
			t.Errorf("Err after a clean iteration = %v, want nil", err)
		}
		if len(got) != len(fakeRows) {
			t.Fatalf("read %d rows, want %d", len(got), len(fakeRows))
		}
		for i, row := range got {
			if row[0] != fakeRows[i][0] || row[1] != fakeRows[i][1] {
				t.Errorf("row %d = %v, want %v", i, row, fakeRows[i])
			}
		}

		if err := rows.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		// Closing twice happens whenever a caller defers Close and also closes
		// on its own path out.
		if err := rows.Close(); err != nil {
			t.Errorf("second Close = %v, want nil", err)
		}
	})

	t.Run("Err reports a broken result set", func(t *testing.T) {
		pool := newPool(OpenFakeDB(t))

		rows, err := pool.Exec(context.Background(), failingQuery)
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}
		for rows.Next() {
		}
		// Next() going false says nothing on its own: it is the end of the
		// rows and a failure halfway through them alike.
		if !errors.Is(rows.Err(), errBoom) {
			t.Errorf("Err = %v, want %v", rows.Err(), errBoom)
		}
		if err := rows.Close(); err != nil {
			t.Errorf("Close after a broken result set = %v, want nil", err)
		}
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		pool := newPool(OpenFakeDB(t))

		if err := pool.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := pool.Close(); err != nil {
			t.Errorf("second Close = %v, want nil", err)
		}
		if _, err := pool.Exec(context.Background(), "SELECT 1"); err == nil {
			t.Error("Exec on a closed pool returned no error")
		}
	})
}

// ── The fake driver ──────────────────────────────────────────────────────────

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(query string) (driver.Stmt, error) { return fakeStmt{query: query}, nil }
func (fakeConn) Close() error                              { return nil }
func (fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("dbtest: transactions are not supported")
}

type fakeStmt struct{ query string }

func (fakeStmt) Close() error                               { return nil }
func (fakeStmt) NumInput() int                              { return 0 }
func (fakeStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(0), nil }
func (s fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return &fakeResultRows{fails: s.query == failingQuery}, nil
}

type fakeResultRows struct {
	pos   int
	fails bool
}

func (*fakeResultRows) Columns() []string { return fakeColumns }
func (*fakeResultRows) Close() error      { return nil }

func (r *fakeResultRows) Next(dest []driver.Value) error {
	if r.fails && r.pos == 1 {
		return errBoom
	}
	if r.pos >= len(fakeRows) {
		return io.EOF
	}
	copy(dest, fakeRows[r.pos])
	r.pos++
	return nil
}
