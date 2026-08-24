//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// chStorageSection is the storage block pointing results at ClickHouse.
func chStorageSection(dsn string) string {
	return fmt.Sprintf("  clickhouse:\n    dsn: %q\n    table: %s\n", dsn, chTable)
}

// TestClickHouse is the whole ClickHouse group. The container and the store
// registration are shared process-wide through ensureClickHouseStore (see
// suite_test.go), the same shape TestS3_RangeDownloadOverREST and
// TestS3_ResultRoundTrip use for "s3": storage.Register panics on a duplicate
// name and offers no unregister, so a container scoped to this test alone
// would strand any other top-level test that also needs "clickhouse".
func TestClickHouse(t *testing.T) {
	dsn := ensureClickHouseStore(t)

	t.Run("ResultStore", func(t *testing.T) { testClickHouseResultStore(t, dsn) })
	t.Run("GarbageCollectionRemovesRows", func(t *testing.T) { testClickHouseGC(t, dsn) })
	t.Run("QueryEngine", func(t *testing.T) { testClickHouseQueryEngine(t, dsn) })
}

// chSourceTable is what the query-engine test reads from. It is a table of its
// own rather than chTable: the same server plays both roles here, results
// store and query target, and the two must not read each other's rows.
const chSourceTable = "dbbridge_source"

// testClickHouseQueryEngine runs a query through the ClickHouse *driver* - the
// engine side of ClickHouse, which the tests above never touch because they
// point `databases` at PostgreSQL and only use ClickHouse to store results.
// Storage stays on the filesystem here for the same reason, so a failure lands
// on the driver rather than on the store.
func testClickHouseQueryEngine(t *testing.T, chDSN string) {
	redisAddr := startRedis(t)
	seedClickHouse(t, chDSN,
		"CREATE TABLE IF NOT EXISTS "+chSourceTable+" (id Int32, name String) ENGINE = Memory",
		"TRUNCATE TABLE "+chSourceTable,
		"INSERT INTO "+chSourceTable+" VALUES (1, 'alice'), (2, 'bob')",
	)

	h := newHarness(t, harnessOptions{
		instanceID: "node-ch-engine",
		redisAddr:  redisAddr,
		databases:  chDatabases(chDSN),
	})
	baseURL := newRESTServer(t, h)

	rec := restDecode(t, restPost(t, baseURL+"/v1/queries",
		`{"database_id":"ch","sql":"SELECT id, name FROM `+chSourceTable+` ORDER BY id","options":{"mode":"sync"}}`))
	if rec.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED", rec.State)
	}
	if rec.Result == nil || rec.Result.RowCount != 2 {
		t.Fatalf("result = %+v, want 2 rows", rec.Result)
	}

	resp, err := http.Get(baseURL + "/v1/queries/" + rec.ID + "/result")
	if err != nil {
		t.Fatalf("GET result: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d JSONL lines, want 2: %q", len(lines), body)
	}
	// The keys come from the driver's RowStream.Columns and the values from its
	// Scan into an untyped destination, so both are checked here.
	for i, want := range []string{"alice", "bob"} {
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &row); err != nil {
			t.Fatalf("parse JSONL %q: %v", lines[i], err)
		}
		if _, ok := row["id"]; !ok {
			t.Errorf("row %d = %q, want an id column", i, lines[i])
		}
		if row["name"] != want {
			t.Errorf("row %d name = %v, want %s", i, row["name"], want)
		}
	}
}

// seedClickHouse creates and fills a table over database/sql rather than
// through db.Pool the way seed() does: the driver's Exec is the query path,
// and clickhouse-go wants DDL and INSERT on ExecContext.
func seedClickHouse(t *testing.T, dsn string, statements ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open clickhouse for seeding: %v", err)
	}
	defer conn.Close()

	for _, stmt := range statements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed clickhouse with %q: %v", stmt, err)
		}
	}
}

// testClickHouseResultStore covers the backend end to end: a query writes its
// rows into ClickHouse, the download reads them back, and parquet is refused
// before anything runs.
func testClickHouseResultStore(t *testing.T, chDSN string) {
	redisAddr := startRedis(t)
	pgDSN := startPostgres(t)
	seed(t, "postgres", pgDSN,
		"CREATE TABLE IF NOT EXISTS people (id int, name text)",
		"INSERT INTO people VALUES (1, 'alice'), (2, 'bob')",
	)

	h := newHarness(t, harnessOptions{
		instanceID:     "node-ch",
		redisAddr:      redisAddr,
		databases:      pgDatabases(pgDSN),
		defaultStorage: "clickhouse",
		storageSection: chStorageSection(chDSN),
	})
	baseURL := newRESTServer(t, h)

	t.Run("jsonl round trip", func(t *testing.T) {
		rec := restDecode(t, restPost(t, baseURL+"/v1/queries",
			`{"database_id":"pg","sql":"SELECT id, name FROM people ORDER BY id","options":{"mode":"sync","result_format":"jsonl"}}`))
		if rec.State != "SUCCEEDED" {
			t.Fatalf("state = %s, want SUCCEEDED", rec.State)
		}
		if rec.Result == nil || rec.Result.Backend != "clickhouse" {
			t.Fatalf("result = %+v, want a clickhouse ref", rec.Result)
		}

		// Read the result back over the REST download route, which goes
		// through the store's Reader and issues its own SELECT against
		// ClickHouse - not a value cached from the write path.
		resp, err := http.Get(baseURL + "/v1/queries/" + rec.ID + "/result")
		if err != nil {
			t.Fatalf("GET result: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		if len(lines) != 2 {
			t.Fatalf("got %d JSONL lines, want 2: %q", len(lines), body)
		}
		if !strings.Contains(lines[0], "alice") || !strings.Contains(lines[1], "bob") {
			t.Errorf("rows came back in the wrong order or content: %q", body)
		}
	})

	t.Run("csv round trip", func(t *testing.T) {
		rec := restDecode(t, restPost(t, baseURL+"/v1/queries",
			`{"database_id":"pg","sql":"SELECT id, name FROM people ORDER BY id","options":{"mode":"sync","result_format":"csv"}}`))
		if rec.State != "SUCCEEDED" {
			t.Fatalf("state = %s, want SUCCEEDED", rec.State)
		}

		resp, err := http.Get(baseURL + "/v1/queries/" + rec.ID + "/result")
		if err != nil {
			t.Fatalf("GET result: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		if len(lines) != 3 {
			t.Fatalf("got %d CSV lines, want header + 2 rows: %q", len(lines), body)
		}
		if lines[0] != "id,name" {
			t.Errorf("CSV header = %q, want id,name", lines[0])
		}
	})

	// The store joins rows back with "\n", which is byte-exact for the
	// line-oriented formats and destroys a parquet file: it came back one byte
	// longer with its PAR1 footer read as "AR1\n".
	t.Run("parquet is refused", func(t *testing.T) {
		resp := restPost(t, baseURL+"/v1/queries",
			`{"database_id":"pg","sql":"SELECT id FROM people","options":{"mode":"sync","result_format":"parquet"}}`)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
	})
}

// testClickHouseGC: expiry has to reach the backend, not just the metadata, or
// results accumulate in ClickHouse for ever.
func testClickHouseGC(t *testing.T, chDSN string) {
	redisAddr := startRedis(t)
	pgDSN := startPostgres(t)
	seed(t, "postgres", pgDSN,
		"CREATE TABLE IF NOT EXISTS people (id int, name text)",
		"INSERT INTO people VALUES (1, 'alice')",
	)

	h := newHarness(t, harnessOptions{
		instanceID:     "node-ch-gc",
		redisAddr:      redisAddr,
		databases:      pgDatabases(pgDSN),
		defaultStorage: "clickhouse",
		storageSection: chStorageSection(chDSN),
		gcInterval:     200 * time.Millisecond,
	})
	baseURL := newRESTServer(t, h)

	rec := restDecode(t, restPost(t, baseURL+"/v1/queries",
		`{"database_id":"pg","sql":"SELECT id, name FROM people","options":{"mode":"sync","result_ttl_seconds":1}}`))
	if rec.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED", rec.State)
	}

	// The TTL is one second and the harness sweeps every 200ms, so the record
	// is gone within roughly 1.2s; the deadline is slack, not an expected wait.
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("the expired ClickHouse result was never collected")
		case <-ticker.C:
			resp, err := http.Get(baseURL + "/v1/queries/" + rec.ID)
			if err != nil {
				t.Fatalf("GET status: %v", err)
			}
			code := resp.StatusCode
			resp.Body.Close()
			if code != http.StatusNotFound {
				continue
			}

			// The metadata 404s only after the manager's GC pass calls
			// store.Delete and it returns without error, but that proves
			// only that the DELETE statement was accepted, not that
			// ClickHouse finished applying it: ALTER TABLE ... DELETE queues
			// an asynchronous mutation there. Query the table directly - not
			// through the store, so nothing here can be served from a cache
			// - and give the mutation a little room to land before failing.
			assertClickHouseRowsGone(t, chDSN, rec.ID)
			return
		}
	}
}

// assertClickHouseRowsGone polls the results table directly until no row
// remains for queryID, or fails once a generous deadline passes.
func assertClickHouseRowsGone(t *testing.T, dsn, queryID string) {
	t.Helper()

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open clickhouse for verification: %v", err)
	}
	defer db.Close()

	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		var n int
		q := fmt.Sprintf("SELECT count() FROM %s WHERE query_id = ?", chTable)
		if err := db.QueryRow(q, queryID).Scan(&n); err != nil {
			t.Fatalf("count clickhouse rows for %s: %v", queryID, err)
		}
		if n == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("clickhouse still has %d row(s) for query %s after GC", n, queryID)
		case <-ticker.C:
		}
	}
}
