package s3

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A network-free S3 object API for this package's tests.
//
// Everything the store does past checkKey is an AWS call, so `make test-unit`
// could only reach the confinement guard - the upload, the download and the head
// were covered by TestS3_ResultRoundTrip alone, behind the Docker-only
// integration job. httptest gives the SDK a real endpoint without MinIO.
//
// The fake models S3 where the store's behaviour depends on it: a missing key
// answers GET and HEAD with 404, and DELETE stays idempotent, as S3 and MinIO
// both do.

const fakeBucket = "dbbridge"

type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	// putStatus, when set, makes every upload fail with that status instead of
	// storing the body.
	putStatus int
}

// newFakeS3 starts the endpoint and returns a store pointed at it.
func newFakeS3(t *testing.T) (*S3ResultStore, *fakeS3) {
	t.Helper()
	fake := &fakeS3{objects: make(map[string][]byte)}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	store, err := NewS3ResultStore(t.Context(), fakeBucket, "eu-central-1", srv.URL, "key", "secret")
	if err != nil {
		t.Fatalf("NewS3ResultStore: %v", err)
	}
	return store, fake
}

// rejectUploads makes the next uploads answer with status. 403 is not retried,
// so the failure reaches the writer at once.
func (f *fakeS3) rejectUploads(status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putStatus = status
}

// seed puts an object into the bucket without going through the store, so a
// test can place one where the store must refuse to reach it.
func (f *fakeS3) seed(key, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = []byte(body)
}

// stored returns the object the fake holds under key.
func (f *fakeS3) stored(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[key]
	return body, ok
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The store sets UsePathStyle, so every request path is /<bucket>/<key>.
	key, ok := strings.CutPrefix(r.URL.Path, "/"+fakeBucket+"/")
	if !ok {
		writeS3Error(w, r, http.StatusNotFound, "NoSuchBucket")
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.Method {
	case http.MethodPut:
		if f.putStatus != 0 {
			writeS3Error(w, r, f.putStatus, "AccessDenied")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeS3Error(w, r, http.StatusBadRequest, "IncompleteBody")
			return
		}
		f.objects[key] = body
		w.Header().Set("ETag", `"fake"`)

	case http.MethodGet, http.MethodHead:
		body, found := f.objects[key]
		if !found {
			writeS3Error(w, r, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		// S3 answers with the checksum it stored, and the SDK validates the body
		// against it. Without the header it only logs that it cannot.
		var crc [4]byte
		binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
		w.Header().Set("x-amz-checksum-crc32", base64.StdEncoding.EncodeToString(crc[:]))
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}

	case http.MethodDelete:
		// S3 deletes are idempotent: a key that is already gone still answers
		// 204. GC re-running over a swept result must not see a failure.
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)

	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

// writeS3Error answers with the XML body the SDK turns into an API error. A HEAD
// response carries no body, so only the status reaches the client there.
func writeS3Error(w http.ResponseWriter, r *http.Request, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}
