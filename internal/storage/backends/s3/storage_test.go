package s3

import (
	"context"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

// The tests here need no bucket: they cover the confinement check §9 Isolation
// asks for ("the s3 backend confines keys to its result prefix"), which runs
// before any AWS call. The fs backend has the same guard covered by
// TestFSResultStoreLocatorConfinement; this is its counterpart, and until now
// the whole package was at zero coverage outside the Docker-only integration
// job.
//
// Locators come from the MetaStore, which is a separate trust domain, so a
// tampered one must not be able to make dbbridge read or delete another object
// in the same bucket.

func TestCheckKeyAcceptsItsOwnLayout(t *testing.T) {
	// Exactly what Writer mints, for every format the API accepts.
	for _, format := range []string{"jsonl", "csv", "parquet"} {
		key := objectPrefix + "5f5ec2a1-0c9e-4a4b-9f7d-1f5a3c4d5e6f." + format
		if err := checkKey(key); err != nil {
			t.Errorf("checkKey(%q) = %v, want nil", key, err)
		}
	}
}

func TestCheckKeyRejectsEscapes(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"no prefix at all", "secrets/credentials"},
		{"absolute path", "/etc/passwd"},
		{"traversal out of the prefix", objectPrefix + "../secrets/credentials"},
		{"nested traversal", objectPrefix + "a/../../secrets"},
		{"absolute inside the prefix", objectPrefix + "/etc/passwd"},
		{"empty", ""},
		{"prefix as a substring, not a prefix", "other/" + objectPrefix + "q.jsonl"},
		{"prefix look-alike", "results-other/q.jsonl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkKey(tt.key); err == nil {
				t.Errorf("checkKey(%q) = nil, want a rejection", tt.key)
			}
		})
	}
}

// TestStoreRejectsAForeignLocator drives the rejection through the ResultStore
// methods rather than the helper, so a future refactor that drops a checkKey
// call is caught. The store has no reachable endpoint: reaching AWS at all
// would be the failure.
func TestStoreRejectsAForeignLocator(t *testing.T) {
	store, err := NewS3ResultStore(context.Background(), "dbbridge", "eu-central-1", "http://127.0.0.1:1", "key", "secret")
	if err != nil {
		t.Fatalf("NewS3ResultStore: %v", err)
	}

	ref := domain.ResultRef{Backend: "s3", Locator: "../../etc/passwd", Format: "jsonl"}

	if _, err := store.Reader(context.Background(), ref); err == nil {
		t.Error("Reader accepted a locator outside the result prefix")
	} else if !strings.Contains(err.Error(), objectPrefix) {
		t.Errorf("Reader error = %v, want it to name the prefix", err)
	}
	if _, err := store.Stat(context.Background(), ref); err == nil {
		t.Error("Stat accepted a locator outside the result prefix")
	}
	if err := store.Delete(context.Background(), ref); err == nil {
		t.Error("Delete accepted a locator outside the result prefix")
	}
}

// TestWriterMintsAConfinedKey pins the locator layout Writer produces. It is the
// value that ends up in the MetaStore and comes back as an untrusted input on
// every download, so it has to satisfy the same check on the way out.
func TestWriterMintsAConfinedKey(t *testing.T) {
	store, err := NewS3ResultStore(context.Background(), "dbbridge", "eu-central-1", "http://127.0.0.1:1", "key", "secret")
	if err != nil {
		t.Fatalf("NewS3ResultStore: %v", err)
	}

	// The upload goroutine starts here and will fail against the dead endpoint;
	// closing the writer is what waits for it, and its error is not what this
	// test is about.
	ctx, cancel := context.WithCancel(context.Background())
	w, ref, err := store.Writer(ctx, "5f5ec2a1-0c9e-4a4b-9f7d-1f5a3c4d5e6f", "jsonl")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	cancel()
	_ = w.Close()

	if ref.Backend != "s3" {
		t.Errorf("ref.Backend = %q, want s3", ref.Backend)
	}
	if ref.Format != "jsonl" {
		t.Errorf("ref.Format = %q, want jsonl", ref.Format)
	}
	if !strings.HasPrefix(ref.Locator, objectPrefix) {
		t.Errorf("ref.Locator = %q, want it under %q", ref.Locator, objectPrefix)
	}
	if err := checkKey(ref.Locator); err != nil {
		t.Errorf("the locator Writer minted does not pass checkKey: %v", err)
	}
}
