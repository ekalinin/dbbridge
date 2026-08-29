package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// A network-free database/sql driver for this package's tests.
//
// The store reaches ClickHouse for everything it does, and its own constructor
// hardcodes the driver name, so `make test-unit` could not get at the chunking,
// the row-group commits or the reader that joins the rows back - the whole
// package sat at zero coverage outside the container job. The fake below keeps
// rows in a map and honours transactions, so the commit boundary the writer
// relies on is a real one here too.

const fakeDriverName = "clickhouse-fake"

func init() { sql.Register(fakeDriverName, &fakeDriver{}) }

// fakeDB is the state one test's handle sees.
type fakeDB struct {
	mu sync.Mutex
	// rows holds the committed data column per query_id, in chunk order.
	rows map[string][]string
	// tables counts CREATE TABLE statements, so a test can tell that the
	// constructor issued one.
	tables int
	// failCreate makes CREATE TABLE fail, for the constructor's error path.
	failCreate bool
	// failInsertAt makes the nth insert (1-based) fail, for the rollback path.
	// Zero never fails.
	failInsertAt int
	inserts      int
}

// current is the fakeDB the next Open call binds to. database/sql caches
// handles by DSN, so each test installs its own before opening.
var current atomic.Pointer[fakeDB]

// openFake registers a fresh state and returns a handle onto it.
func openFake(t *testing.T, state *fakeDB) *sql.DB {
	t.Helper()
	if state.rows == nil {
		state.rows = make(map[string][]string)
	}
	current.Store(state)
	handle, err := sql.Open(fakeDriverName, "")
	if err != nil {
		t.Fatalf("open %s: %v", fakeDriverName, err)
	}
	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Errorf("close fake handle: %v", err)
		}
	})
	return handle
}

// newFakeStore builds a ClickHouseResultStore over a fake handle.
func newFakeStore(t *testing.T, state *fakeDB) *ClickHouseResultStore {
	t.Helper()
	store, err := newStore(openFake(t, state), "dbbridge_results")
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	return store
}

// committed returns the rows the fake holds for a query id.
func (f *fakeDB) committed(queryID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.rows[queryID]...)
}

type fakeDriver struct{}

func (*fakeDriver) Open(string) (driver.Conn, error) {
	state := current.Load()
	if state == nil {
		return nil, errors.New("clickhouse-fake: no state installed")
	}
	return &fakeConn{state: state}, nil
}

type fakeConn struct {
	state *fakeDB
	// tx holds the rows of an open transaction until it commits.
	tx map[string][]string
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{conn: c, query: query}, nil
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	c.tx = make(map[string][]string)
	return &fakeTx{conn: c}, nil
}

type fakeTx struct{ conn *fakeConn }

func (t *fakeTx) Commit() error {
	c := t.conn
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	for id, lines := range c.tx {
		c.state.rows[id] = append(c.state.rows[id], lines...)
	}
	c.tx = nil
	return nil
}

func (t *fakeTx) Rollback() error {
	t.conn.tx = nil
	return nil
}

type fakeStmt struct {
	conn  *fakeConn
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	q := strings.TrimSpace(s.query)
	c := s.conn

	switch {
	case strings.HasPrefix(q, "CREATE TABLE"):
		c.state.mu.Lock()
		c.state.tables++
		fail := c.state.failCreate
		c.state.mu.Unlock()
		if fail {
			return nil, errors.New("clickhouse-fake: create table rejected")
		}
		return driver.RowsAffected(0), nil

	case strings.HasPrefix(q, "INSERT INTO"):
		if len(args) != 4 {
			return nil, fmt.Errorf("clickhouse-fake: insert wants 4 args, got %d", len(args))
		}
		c.state.mu.Lock()
		c.state.inserts++
		fail := c.state.failInsertAt > 0 && c.state.inserts == c.state.failInsertAt
		c.state.mu.Unlock()
		if fail {
			return nil, errors.New("clickhouse-fake: insert rejected")
		}
		id, _ := args[0].(string)
		data, _ := args[3].(string)
		if c.tx == nil {
			// Outside a transaction the row lands immediately.
			c.state.mu.Lock()
			c.state.rows[id] = append(c.state.rows[id], data)
			c.state.mu.Unlock()
		} else {
			c.tx[id] = append(c.tx[id], data)
		}
		return driver.RowsAffected(1), nil

	case strings.HasPrefix(q, "ALTER TABLE"):
		id, _ := args[0].(string)
		c.state.mu.Lock()
		delete(c.state.rows, id)
		c.state.mu.Unlock()
		return driver.RowsAffected(1), nil
	}

	return nil, fmt.Errorf("clickhouse-fake: unexpected exec %q", q)
}

func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	q := strings.TrimSpace(s.query)
	id, _ := args[0].(string)
	lines := s.conn.state.committed(id)

	switch {
	case strings.HasPrefix(q, "SELECT data FROM"):
		values := make([][]driver.Value, 0, len(lines))
		for _, line := range lines {
			values = append(values, []driver.Value{line})
		}
		return &fakeRows{cols: []string{"data"}, values: values}, nil

	case strings.HasPrefix(q, "SELECT count()"):
		var size int64
		for _, line := range lines {
			size += int64(len(line))
		}
		return &fakeRows{
			cols:   []string{"count()", "sum(length(data))"},
			values: [][]driver.Value{{int64(len(lines)), size}},
		}, nil
	}

	return nil, fmt.Errorf("clickhouse-fake: unexpected query %q", q)
}

type fakeRows struct {
	cols   []string
	values [][]driver.Value
	pos    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.pos])
	r.pos++
	return nil
}
