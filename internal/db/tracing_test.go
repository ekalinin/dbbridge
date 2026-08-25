package db_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/testutil"

	"go.opentelemetry.io/otel/codes"
)

// fakePool answers Exec with rows, or with an error when one is set.
type fakePool struct {
	execErr   error
	rows      int
	streamErr error
	closed    bool
}

func (p *fakePool) Exec(context.Context, string) (db.RowStream, error) {
	if p.execErr != nil {
		return nil, p.execErr
	}
	return &fakeRowStream{pool: p, left: p.rows}, nil
}

func (p *fakePool) Ping(context.Context) error { return nil }
func (p *fakePool) Stat() db.PoolStat          { return db.PoolStat{} }
func (p *fakePool) Close() error               { return nil }

type fakeRowStream struct {
	pool *fakePool
	left int
}

func (s *fakeRowStream) Columns() ([]string, error) { return []string{"id"}, nil }
func (s *fakeRowStream) Scan(...any) error          { return nil }
func (s *fakeRowStream) Err() error                 { return s.pool.streamErr }

func (s *fakeRowStream) Next() bool {
	if s.left == 0 {
		return false
	}
	s.left--
	return true
}

func (s *fakeRowStream) Close() error {
	s.pool.closed = true
	return nil
}

// drain consumes the stream the way EncodeStream does.
func drain(t *testing.T, stream db.RowStream) {
	t.Helper()
	for stream.Next() {
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

// TestTraced_SplitsExecutionFromRowPull: the two halves fail differently and
// take their time differently - submitting the statement is not the same
// operation as pulling the result set, which runs against the write to storage.
func TestTraced_SplitsExecutionFromRowPull(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	pool := db.Traced(&fakePool{rows: 3}, "postgres")

	stream, err := pool.Exec(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	drain(t, stream)

	exec := testutil.SpanByName(t, recorder, "db.exec")
	rows := testutil.SpanByName(t, recorder, "db.rows")
	if exec.Parent().SpanID() != rows.Parent().SpanID() {
		t.Errorf("db.exec parent %s and db.rows parent %s differ, want siblings: "+
			"db.exec is over before the first row is pulled",
			exec.Parent().SpanID(), rows.Parent().SpanID())
	}

	var read int64 = -1
	for _, attr := range rows.Attributes() {
		if attr.Key == "db.rows_read" {
			read = attr.Value.AsInt64()
		}
	}
	if read != 3 {
		t.Errorf("db.rows_read = %d, want 3", read)
	}
}

// TestTraced_FailedExecOpensNoRowSpan: there is no result set to pull, and a
// db.rows span that never ends is a span nothing ever exports.
func TestTraced_FailedExecOpensNoRowSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	want := errors.New("relation does not exist")
	pool := db.Traced(&fakePool{execErr: want}, "postgres")

	if _, err := pool.Exec(context.Background(), "SELECT 1"); !errors.Is(err, want) {
		t.Fatalf("Exec error = %v, want %v", err, want)
	}

	if span := testutil.SpanByName(t, recorder, "db.exec"); span.Status().Code != codes.Error {
		t.Errorf("db.exec status = %v, want Error", span.Status().Code)
	}
	for _, name := range testutil.SpanNames(recorder) {
		if name == "db.rows" {
			t.Error("a failed Exec opened a db.rows span")
		}
	}
}

// TestTraced_RowErrorMarksTheSpanButNotClose: a stream that stopped early
// belongs on the span, while Close's own result is what the caller logs as a
// close failure - and a mid-stream error is not one.
func TestTraced_RowErrorMarksTheSpanButNotClose(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	inner := &fakePool{rows: 1, streamErr: errors.New("connection reset")}
	pool := db.Traced(inner, "postgres")

	stream, err := pool.Exec(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	for stream.Next() {
	}
	if err := stream.Close(); err != nil {
		t.Errorf("Close returned %v, want the wrapped stream's nil", err)
	}
	if !inner.closed {
		t.Error("the wrapper did not close the stream underneath it")
	}

	if span := testutil.SpanByName(t, recorder, "db.rows"); span.Status().Code != codes.Error {
		t.Errorf("db.rows status = %v, want Error", span.Status().Code)
	}
}

// TestTraced_UnwrapReturnsTheDriversPool keeps the wrapper from hiding the
// concrete driver type from a caller that needs it.
func TestTraced_UnwrapReturnsTheDriversPool(t *testing.T) {
	inner := &fakePool{}

	if got := db.Unwrap(db.Traced(inner, "postgres")); got != db.Pool(inner) {
		t.Errorf("Unwrap returned %#v, want the wrapped pool", got)
	}
	if got := db.Unwrap(inner); got != db.Pool(inner) {
		t.Errorf("Unwrap of an unwrapped pool returned %#v, want it unchanged", got)
	}
}
