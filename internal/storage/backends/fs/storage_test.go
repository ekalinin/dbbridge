package fs

import (
	"io"
	"testing"
)

func TestFSResultStoreRoundTripAndStat(t *testing.T) {
	store, err := NewFSResultStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSResultStore: %v", err)
	}
	ctx := t.Context()

	w, ref, err := store.Writer(ctx, "query-1", "jsonl")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	const payload = "{\"id\":1}\n{\"id\":2}\n"
	if _, err := io.WriteString(w, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	statRef, err := store.Stat(ctx, ref)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if statRef.SizeBytes != int64(len(payload)) {
		t.Errorf("Stat SizeBytes = %d, want %d", statRef.SizeBytes, len(payload))
	}

	r, err := store.Reader(ctx, ref)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if string(got) != payload {
		t.Errorf("read = %q, want %q", got, payload)
	}

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Stat(ctx, ref); err == nil {
		t.Error("expected Stat to fail after Delete")
	}
}
