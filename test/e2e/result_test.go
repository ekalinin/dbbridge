package e2e_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// submitSync submits a synchronous query and returns its record.
func submitSync(t *testing.T, h *testHarness, opts map[string]any) queryRecord {
	t.Helper()
	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT id, name FROM users",
		Options:    opts,
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusOK)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	return rec
}

// TestResult_ParquetRoundTrip: the stored bytes have to be a parquet file a
// standard reader accepts, not just something the encoder was willing to write.
func TestResult_ParquetRoundTrip(t *testing.T) {
	h := newHarness(t)
	rec := submitSync(t, h, map[string]any{"mode": "sync", "result_format": "parquet"})

	if rec.Result == nil || rec.Result.Format != "parquet" {
		t.Fatalf("result = %+v, want a parquet ref", rec.Result)
	}

	dl := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result")
	defer dl.Body.Close()
	assertStatus(t, dl, http.StatusOK)

	body, err := io.ReadAll(dl.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(body) < 8 || string(body[:4]) != "PAR1" || string(body[len(body)-4:]) != "PAR1" {
		t.Fatalf("body is not a parquet file: %d bytes, head %q", len(body), body[:min(8, len(body))])
	}

	file, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if got := file.NumRows(); got != 2 {
		t.Fatalf("NumRows = %d, want 2", got)
	}

	names := make(map[string]bool)
	for _, f := range file.Schema().Fields() {
		names[f.Name()] = true
	}
	for _, want := range []string{"id", "name"} {
		if !names[want] {
			t.Errorf("schema is missing column %q", want)
		}
	}
}

// TestResult_UnknownFormatIsRejected: the format is checked against a whitelist
// before any storage writer is opened.
func TestResult_UnknownFormatIsRejected(t *testing.T) {
	h := newHarness(t)

	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync", "result_format": "../../etc/passwd"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusBadRequest)
}

// TestResult_FormatTheBackendCannotHoldIsRejected: the line-oriented store
// splits the stream on newlines, which is byte-exact for JSONL and CSV and
// destroys a parquet file. The pair is checked at submission time rather than
// after a query has already run.
func TestResult_FormatTheBackendCannotHoldIsRejected(t *testing.T) {
	h := newHarness(t)

	rejected := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync", "result_format": "parquet", "storage_backend": "clickhouse"},
	})
	defer rejected.Body.Close()
	assertStatus(t, rejected, http.StatusBadRequest)

	accepted := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "testdb",
		SQL:        "SELECT 1",
		Options:    map[string]any{"mode": "sync", "result_format": "jsonl", "storage_backend": "clickhouse"},
	})
	defer accepted.Body.Close()
	assertStatus(t, accepted, http.StatusOK)
}

// TestResult_RangeRequests covers the HTTP Range contract on the download route.
func TestResult_RangeRequests(t *testing.T) {
	h := newHarness(t)
	rec := submitSync(t, h, map[string]any{"mode": "sync", "result_format": "jsonl"})

	whole := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result")
	full, err := io.ReadAll(whole.Body)
	whole.Body.Close()
	if err != nil {
		t.Fatalf("read full body: %v", err)
	}
	size := rec.Result.SizeBytes
	if size != int64(len(full)) {
		t.Fatalf("result ref says %d bytes, download returned %d", size, len(full))
	}

	rangeGet := func(spec string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, h.baseURL+"/v1/queries/"+rec.ID+"/result", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Range", spec)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET result: %v", err)
		}
		return resp
	}

	t.Run("closed range", func(t *testing.T) {
		resp := rangeGet("bytes=0-4")
		defer resp.Body.Close()
		assertStatus(t, resp, http.StatusPartialContent)
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes 0-4/%d", size); got != want {
			t.Errorf("Content-Range = %q, want %q", got, want)
		}
		body, _ := io.ReadAll(resp.Body)
		if !bytes.Equal(body, full[:5]) {
			t.Errorf("body = %q, want %q", body, full[:5])
		}
	})

	t.Run("open ended range", func(t *testing.T) {
		resp := rangeGet("bytes=5-")
		defer resp.Body.Close()
		assertStatus(t, resp, http.StatusPartialContent)
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes 5-%d/%d", size-1, size); got != want {
			t.Errorf("Content-Range = %q, want %q", got, want)
		}
		body, _ := io.ReadAll(resp.Body)
		if !bytes.Equal(body, full[5:]) {
			t.Errorf("body = %q, want %q", body, full[5:])
		}
	})

	t.Run("past the end is unsatisfiable", func(t *testing.T) {
		resp := rangeGet(fmt.Sprintf("bytes=%d-", size+10))
		defer resp.Body.Close()
		assertStatus(t, resp, http.StatusRequestedRangeNotSatisfiable)
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes */%d", size); got != want {
			t.Errorf("Content-Range = %q, want %q", got, want)
		}
	})

	t.Run("malformed range", func(t *testing.T) {
		resp := rangeGet("items=0-4")
		defer resp.Body.Close()
		assertStatus(t, resp, http.StatusRequestedRangeNotSatisfiable)
	})
}

// TestResult_OffsetAndLimit covers the query-parameter form of partial reads,
// which returns 200 rather than 206 because it is not an HTTP range.
func TestResult_OffsetAndLimit(t *testing.T) {
	h := newHarness(t)
	rec := submitSync(t, h, map[string]any{"mode": "sync", "result_format": "jsonl"})

	whole := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result")
	full, err := io.ReadAll(whole.Body)
	whole.Body.Close()
	if err != nil {
		t.Fatalf("read full body: %v", err)
	}

	sliced := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result?offset=2&limit=6")
	defer sliced.Body.Close()
	assertStatus(t, sliced, http.StatusOK)
	body, _ := io.ReadAll(sliced.Body)
	if !bytes.Equal(body, full[2:8]) {
		t.Errorf("body = %q, want %q", body, full[2:8])
	}

	for _, bad := range []string{"?offset=-1", "?limit=-1", "?offset=abc"} {
		resp := get(t, h.baseURL+"/v1/queries/"+rec.ID+"/result"+bad)
		assertStatus(t, resp, http.StatusBadRequest)
		resp.Body.Close()
	}
}
