package s3

import (
	"context"
	"io"
	"net/http"
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
// call is caught. The bucket holds an object outside the result prefix, so a
// method that skipped the check would read it and report success instead of an
// error - the same shape as TestFSResultStoreLocatorConfinement.
func TestStoreRejectsAForeignLocator(t *testing.T) {
	store, fake := newFakeS3(t)
	const victim = "secrets/credentials"
	fake.seed(victim, "keep me")
	ctx := t.Context()

	ref := domain.ResultRef{Backend: "s3", Locator: victim, Format: "jsonl"}

	if r, err := store.Reader(ctx, ref); err == nil {
		_ = r.Close()
		t.Error("Reader accepted a locator outside the result prefix")
	} else if !strings.Contains(err.Error(), objectPrefix) {
		t.Errorf("Reader error = %v, want it to name the prefix", err)
	}
	if _, err := store.Stat(ctx, ref); err == nil {
		t.Error("Stat accepted a locator outside the result prefix")
	}
	if err := store.Delete(ctx, ref); err == nil {
		t.Error("Delete accepted a locator outside the result prefix")
	}
	if _, ok := fake.stored(victim); !ok {
		t.Error("the object outside the result prefix was deleted")
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

// TestRoundTripThroughStat covers the contract §5.3 states: Writer mints the
// ResultRef rather than receiving one, so the ref it hands back is the only
// thing a later download has to work from. Stat has to describe the same object,
// and Reader has to return the bytes Writer took.
func TestRoundTripThroughStat(t *testing.T) {
	store, fake := newFakeS3(t)
	ctx := t.Context()

	w, ref, err := store.Writer(ctx, "q1", "jsonl")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	const payload = "{\"id\":1}\n{\"id\":2}\n"
	if _, err := io.WriteString(w, payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	// Close is where the upload finishes, so it is what decides the query's
	// verdict (I4).
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	if body, ok := fake.stored(ref.Locator); !ok {
		t.Fatalf("nothing was uploaded under %q", ref.Locator)
	} else if string(body) != payload {
		t.Errorf("uploaded %q, want %q", body, payload)
	}

	got, err := store.Stat(ctx, ref)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got.Backend != ref.Backend || got.Locator != ref.Locator || got.Format != ref.Format {
		t.Errorf("Stat returned %+v, want it to keep %+v", got, ref)
	}
	if got.SizeBytes != int64(len(payload)) {
		t.Errorf("Stat SizeBytes = %d, want %d", got.SizeBytes, len(payload))
	}

	r, err := store.Reader(ctx, ref)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("close reader: %v", err)
	}
	if string(body) != payload {
		t.Errorf("read %q, want %q", body, payload)
	}

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := fake.stored(ref.Locator); ok {
		t.Error("the object survived Delete")
	}
	if _, err := store.Stat(ctx, ref); err == nil {
		t.Error("Stat succeeded after Delete")
	}
}

// TestWriterCloseReportsAFailedUpload: the upload runs in its own goroutine, so
// Close is the only place its error can surface, and §5.3 makes that error the
// one QueryManager turns into a failed query. A rejected upload reported as
// success would leave a SUCCEEDED record pointing at nothing.
func TestWriterCloseReportsAFailedUpload(t *testing.T) {
	store, fake := newFakeS3(t)
	fake.rejectUploads(http.StatusForbidden)

	w, ref, err := store.Writer(t.Context(), "q-denied", "jsonl")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := io.WriteString(w, "{\"id\":1}\n"); err != nil {
		// The pipe is closed with the upload error, so the write may fail here
		// instead - either way the failure has to reach Close below.
		t.Logf("write after the rejected upload: %v", err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close reported success after the upload was rejected")
	}
	if _, ok := fake.stored(ref.Locator); ok {
		t.Error("an object was stored despite the rejection")
	}
}

// TestMissingLocatorIsNotAnEmptySuccess covers the ref whose object is gone -
// swept by GC, or left behind by a MetaStore that outlived the bucket. Reading
// it has to fail rather than serve an empty body under a 200.
func TestMissingLocatorIsNotAnEmptySuccess(t *testing.T) {
	store, _ := newFakeS3(t)
	ctx := t.Context()
	ref := domain.ResultRef{Backend: "s3", Locator: objectPrefix + "q-missing.jsonl", Format: "jsonl"}

	if r, err := store.Reader(ctx, ref); err == nil {
		_ = r.Close()
		t.Error("Reader returned a stream for an object that does not exist")
	}
	if _, err := store.Stat(ctx, ref); err == nil {
		t.Error("Stat succeeded on an object that does not exist")
	}
	// Delete stays idempotent, as it is on fs and in S3 itself: a GC sweep that
	// runs twice over the same result must not report a failure.
	if err := store.Delete(ctx, ref); err != nil {
		t.Errorf("Delete of a missing object: %v", err)
	}
}
