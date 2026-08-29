package clickhouse

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

// write streams payload through the store's Writer and returns the ResultRef it
// minted.
func write(t *testing.T, store *ClickHouseResultStore, queryID, format, payload string) domain.ResultRef {
	t.Helper()
	w, ref, err := store.Writer(context.Background(), queryID, format)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := io.WriteString(w, payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	// Close is where the insert is committed and its error surfaces, so the
	// query's verdict hangs on it (I4).
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return ref
}

func read(t *testing.T, store *ClickHouseResultStore, ref domain.ResultRef) string {
	t.Helper()
	r, err := store.Reader(context.Background(), ref)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	return string(body)
}

// TestSupportsFormat pins the contract the submission path checks. Only JSONL
// survives a write followed by a read byte for byte: the store splits the stream
// on newlines and joins it back the same way.
func TestSupportsFormat(t *testing.T) {
	store := &ClickHouseResultStore{}
	for format, want := range map[string]bool{
		"jsonl":   true,
		"csv":     false,
		"parquet": false,
		"":        false,
		"JSONL":   false,
	} {
		if got := store.SupportsFormat(format); got != want {
			t.Errorf("SupportsFormat(%q) = %t, want %t", format, got, want)
		}
	}
}

// TestNewStoreCreatesTheTable: the constructor is the only place the schema is
// ever issued, so a deployment against a fresh database depends on it.
func TestNewStoreCreatesTheTable(t *testing.T) {
	state := &fakeDB{}
	store := newFakeStore(t, state)
	if store.table != "dbbridge_results" {
		t.Errorf("table = %q, want the default dbbridge_results", store.table)
	}
	if state.tables != 1 {
		t.Errorf("CREATE TABLE issued %d times, want 1", state.tables)
	}
}

// TestRoundTripIsByteExact is the property SupportsFormat promises for JSONL:
// what Writer took has to be what Reader gives back, or the recorded Checksum
// and SizeBytes stop describing the served bytes (I4).
func TestRoundTripIsByteExact(t *testing.T) {
	store := newFakeStore(t, &fakeDB{})

	payload := `{"id":1,"name":"alice"}` + "\n" +
		`{"id":2,"name":"bob with a \"quote\" and a \r carriage return"}` + "\n"

	ref := write(t, store, "q1", "jsonl", payload)
	if ref.Backend != "clickhouse" {
		t.Errorf("ref.Backend = %q, want clickhouse", ref.Backend)
	}
	if ref.Locator != "q1" {
		t.Errorf("ref.Locator = %q, want the query id", ref.Locator)
	}

	if got := read(t, store, ref); got != payload {
		t.Errorf("round trip is not byte-exact:\n got %q\nwant %q", got, payload)
	}
}

// TestWriterCommitsEveryRowGroup covers the boundary the writer keeps to avoid
// one transaction per result: it commits every 500 rows, so a result larger than
// that lands in several.
func TestWriterCommitsEveryRowGroup(t *testing.T) {
	state := &fakeDB{}
	store := newFakeStore(t, state)

	const rows = 1201
	var payload strings.Builder
	for i := range rows {
		fmt.Fprintf(&payload, `{"n":%d}`+"\n", i)
	}

	ref := write(t, store, "q-big", "jsonl", payload.String())

	if got := len(state.committed("q-big")); got != rows {
		t.Fatalf("stored %d rows, want %d", got, rows)
	}
	if got := read(t, store, ref); got != payload.String() {
		t.Error("a result spanning several row groups did not come back unchanged")
	}
}

// TestWriterReportsAFailedInsert: Close is what decides whether the query
// succeeded, so an insert that fails midway has to surface there rather than
// leaving a SUCCEEDED query pointing at a partial result.
func TestWriterReportsAFailedInsert(t *testing.T) {
	store := newFakeStore(t, &fakeDB{failInsertAt: 2})

	w, _, err := store.Writer(context.Background(), "q-fail", "jsonl")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := io.WriteString(w, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"); err != nil {
		// The pipe is closed with the insert error, so the write may fail here
		// instead - either way the failure has to reach Close below.
		t.Logf("write after the failed insert: %v", err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close reported success after an insert failed")
	}
}

// TestStatCountsWhatWasStored: Stat is the only way to size a result without
// reading it.
func TestStatCountsWhatWasStored(t *testing.T) {
	store := newFakeStore(t, &fakeDB{})
	payload := "{\"n\":1}\n{\"n\":22}\n"
	ref := write(t, store, "q-stat", "jsonl", payload)

	got, err := store.Stat(context.Background(), ref)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", got.RowCount)
	}
	// The stored rows carry no newline, so the size is the payload minus one
	// separator per row.
	if want := int64(len(payload) - 2); got.SizeBytes != want {
		t.Errorf("SizeBytes = %d, want %d", got.SizeBytes, want)
	}
}

// TestDeleteRemovesTheResult covers what GC calls once a result has expired.
func TestDeleteRemovesTheResult(t *testing.T) {
	state := &fakeDB{}
	store := newFakeStore(t, state)
	ref := write(t, store, "q-gone", "jsonl", "{\"n\":1}\n")

	if err := store.Delete(context.Background(), ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := state.committed("q-gone"); len(got) != 0 {
		t.Errorf("%d rows survived the delete", len(got))
	}
	if got := read(t, store, ref); got != "" {
		t.Errorf("reading a deleted result returned %q, want nothing", got)
	}
}

// TestReaderCloseIsIdempotent: the download path closes the reader on a deferred
// call and the streaming goroutine closes the pipe too, so a second Close must
// not panic on an already-closed channel.
func TestReaderCloseIsIdempotent(t *testing.T) {
	store := newFakeStore(t, &fakeDB{})
	ref := write(t, store, "q-close", "jsonl", "{\"n\":1}\n")

	r, err := store.Reader(context.Background(), ref)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
