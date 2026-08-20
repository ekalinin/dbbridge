package fs

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ekalinin/dbbridge/internal/core/domain"
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

// TestFSResultStoreWriterRejectsTraversal covers the path that turned an
// unvalidated result_format into "create, truncate and delete any file".
func TestFSResultStoreWriterRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(outside, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("seed victim: %v", err)
	}

	store, err := NewFSResultStore(root)
	if err != nil {
		t.Fatalf("NewFSResultStore: %v", err)
	}

	rel, err := filepath.Rel(root, outside)
	if err != nil {
		t.Fatalf("rel: %v", err)
	}

	if _, _, err := store.Writer(t.Context(), "query-1", rel); err == nil {
		t.Fatal("Writer accepted a traversing format")
	}
	if _, _, err := store.Writer(t.Context(), "query-1", "../escape"); err == nil {
		t.Fatal("Writer accepted ../escape as a format")
	}

	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "keep me" {
		t.Fatalf("victim file was touched: data=%q err=%v", data, err)
	}
}

// TestFSResultStoreLocatorConfinement guards the read side: Locator comes from
// the MetaStore, so it is untrusted input too.
func TestFSResultStoreLocatorConfinement(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(outside, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("seed victim: %v", err)
	}

	store, err := NewFSResultStore(root)
	if err != nil {
		t.Fatalf("NewFSResultStore: %v", err)
	}

	ref := domain.ResultRef{Backend: "fs", Locator: outside, Format: "jsonl"}
	if _, err := store.Reader(t.Context(), ref); err == nil {
		t.Error("Reader opened a file outside the root")
	}
	if _, err := store.Stat(t.Context(), ref); err == nil {
		t.Error("Stat reached a file outside the root")
	}
	if err := store.Delete(t.Context(), ref); err == nil {
		t.Error("Delete reached a file outside the root")
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("victim file was removed: %v", err)
	}
}
