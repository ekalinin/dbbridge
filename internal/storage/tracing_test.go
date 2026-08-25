package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/testutil"

	"go.opentelemetry.io/otel/codes"
)

// memStore is the minimum a ResultStore has to be for the tracing wrapper to
// have something to wrap.
type memStore struct {
	buf     bytes.Buffer
	readErr error
	// reader, when set, is handed back by Reader instead of one over buf - for
	// the test that checks an io.WriterTo survives the wrapper.
	reader io.ReadCloser
}

func (s *memStore) Writer(context.Context, string, string) (io.WriteCloser, domain.ResultRef, error) {
	return nopWriteCloser{&s.buf}, domain.ResultRef{Backend: "mem", Locator: "obj"}, nil
}

func (s *memStore) Reader(context.Context, domain.ResultRef) (io.ReadCloser, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.reader != nil {
		return s.reader, nil
	}
	return io.NopCloser(bytes.NewReader(s.buf.Bytes())), nil
}

func (s *memStore) Stat(_ context.Context, ref domain.ResultRef) (domain.ResultRef, error) {
	return ref, nil
}

func (s *memStore) Delete(context.Context, domain.ResultRef) error { return nil }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// textOnly is a backend that cannot hold every format losslessly, like the
// ClickHouse store.
type textOnly struct{ *memStore }

func (textOnly) SupportsFormat(format string) bool { return format != "parquet" }

// TestTraced_ForwardsSupportsFormat: the wrapper is its own type, so a backend
// implementing FormatChecker stops being recognized as one unless the wrapper
// forwards the check - and a lossy store/format pair would then pass admission
// and only surface as a result whose checksum no longer matches.
func TestTraced_ForwardsSupportsFormat(t *testing.T) {
	traced := storage.Traced(textOnly{&memStore{}}, "mem")

	if storage.SupportsFormat(traced, "parquet") {
		t.Error("SupportsFormat(parquet) = true through the wrapper, want the backend's false")
	}
	if !storage.SupportsFormat(traced, "jsonl") {
		t.Error("SupportsFormat(jsonl) = false through the wrapper, want the backend's true")
	}
}

// TestTraced_UnwrapReturnsTheRegisteredStore keeps the wrapper from hiding the
// concrete backend from a caller that needs it.
func TestTraced_UnwrapReturnsTheRegisteredStore(t *testing.T) {
	inner := &memStore{}

	if got := storage.Unwrap(storage.Traced(inner, "mem")); got != storage.ResultStore(inner) {
		t.Errorf("Unwrap returned %#v, want the wrapped store", got)
	}
	if got := storage.Unwrap(inner); got != storage.ResultStore(inner) {
		t.Errorf("Unwrap of an unwrapped store returned %#v, want it unchanged", got)
	}
}

// TestTraced_WriteSpanEndsAtClose: Close is where S3 and ClickHouse wait for
// the upload to land (I4), so a span that ended when the last Write returned
// would leave out the part that takes the time.
func TestTraced_WriteSpanEndsAtClose(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	traced := storage.Traced(&memStore{}, "mem")

	w, _, err := traced.Writer(context.Background(), "q-1", "jsonl")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if names := testutil.SpanNames(recorder); len(names) != 0 {
		t.Fatalf("spans %v ended before the writer was closed, want none", names)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	span := testutil.SpanByName(t, recorder, "storage.write")

	var bytesWritten int64 = -1
	for _, attr := range span.Attributes() {
		if attr.Key == "storage.bytes_written" {
			bytesWritten = attr.Value.AsInt64()
		}
	}
	if bytesWritten != 5 {
		t.Errorf("storage.bytes_written = %d, want 5", bytesWritten)
	}
}

// TestTraced_ReaderKeepsTheWriteToFastPath: without WriteTo on the wrapper, a
// local result download loses the io.WriterTo an *os.File carries and copies
// through a buffer instead.
func TestTraced_ReaderKeepsTheWriteToFastPath(t *testing.T) {
	testutil.RecordSpans(t)
	inner := &writerToReadCloser{Reader: strings.NewReader("payload")}
	traced := storage.Traced(&memStore{reader: inner}, "mem")

	r, err := traced.Reader(context.Background(), domain.ResultRef{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	var sink bytes.Buffer
	if _, err := io.Copy(&sink, r); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if !inner.usedWriteTo {
		t.Error("io.Copy did not reach the backend's WriteTo through the tracing wrapper")
	}
	if sink.String() != "payload" {
		t.Errorf("copied %q, want %q", sink.String(), "payload")
	}
}

// writerToReadCloser records whether io.Copy took its WriteTo.
type writerToReadCloser struct {
	*strings.Reader
	usedWriteTo bool
}

func (r *writerToReadCloser) WriteTo(w io.Writer) (int64, error) {
	r.usedWriteTo = true
	return r.Reader.WriteTo(w)
}

func (r *writerToReadCloser) Close() error { return nil }

// TestTraced_FailedOpenIsRecordedOnTheSpan: a backend that cannot even open a
// reader has to end its span rather than leave one hanging.
func TestTraced_FailedOpenIsRecordedOnTheSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	want := errors.New("no such object")
	traced := storage.Traced(&memStore{readErr: want}, "mem")

	if _, err := traced.Reader(context.Background(), domain.ResultRef{}); !errors.Is(err, want) {
		t.Fatalf("Reader error = %v, want %v", err, want)
	}

	span := testutil.SpanByName(t, recorder, "storage.read")
	if span.Status().Code != codes.Error {
		t.Errorf("span status = %v, want Error", span.Status().Code)
	}
}
