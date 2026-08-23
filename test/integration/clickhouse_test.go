//go:build integration

package integration

import (
	"database/sql"
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
