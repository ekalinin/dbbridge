package storage

import (
	"context"
	"io"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracedStore sits between the manager and a result backend so the time a query
// spends writing its result is a span of its own, next to the database spans
// rather than folded into them (spec §11). It is applied by GetStore, so every
// registered backend is instrumented once instead of three times.
type tracedStore struct {
	ResultStore
	backend string
}

// Traced wraps store so its operations produce spans. It is exported for the
// callers that hold a store without going through GetStore.
func Traced(store ResultStore, backend string) ResultStore {
	if store == nil {
		return nil
	}
	return tracedStore{ResultStore: store, backend: backend}
}

// Unwrap returns the store that was registered. A caller that needs the
// concrete backend type goes through this rather than around the registry.
func (s tracedStore) Unwrap() ResultStore { return s.ResultStore }

// Unwrap returns the store underneath the tracing wrapper, or store itself when
// there is none. It is what a caller uses to get at a concrete backend type.
func Unwrap(store ResultStore) ResultStore {
	if u, ok := store.(interface{ Unwrap() ResultStore }); ok {
		return u.Unwrap()
	}
	return store
}

// SupportsFormat forwards the format check. Embedding would not: the wrapper is
// its own type, so a backend that implements FormatChecker would stop being
// recognized as one and a lossy store/format pair would pass admission.
func (s tracedStore) SupportsFormat(format string) bool {
	return SupportsFormat(s.ResultStore, format)
}

func (s tracedStore) Writer(ctx context.Context, queryID string, format string) (w io.WriteCloser, ref domain.ResultRef, err error) {
	// The span covers opening the writer as well as the bytes: for S3 that is
	// where the multipart upload is created, and it fails on its own.
	ctx, span := telemetry.Tracer().Start(ctx, "storage.write",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("storage.backend", s.backend),
			attribute.String("query.id", queryID),
			attribute.String("result.format", format),
		))

	w, ref, err = s.ResultStore.Writer(ctx, queryID, format)
	if err != nil {
		telemetry.EndSpan(span, &err)
		return nil, ref, err
	}
	return &tracedWriteCloser{WriteCloser: w, span: span}, ref, nil
}

func (s tracedStore) Reader(ctx context.Context, ref domain.ResultRef) (r io.ReadCloser, err error) {
	ctx, span := telemetry.Tracer().Start(ctx, "storage.read",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("storage.backend", s.backend),
			attribute.String("result.locator", ref.Locator),
		))

	r, err = s.ResultStore.Reader(ctx, ref)
	if err != nil {
		telemetry.EndSpan(span, &err)
		return nil, err
	}
	return &tracedReadCloser{ReadCloser: r, span: span}, nil
}

func (s tracedStore) Stat(ctx context.Context, ref domain.ResultRef) (out domain.ResultRef, err error) {
	ctx, span := telemetry.Tracer().Start(ctx, "storage.stat",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("storage.backend", s.backend),
			attribute.String("result.locator", ref.Locator),
		))
	defer telemetry.EndSpan(span, &err)

	return s.ResultStore.Stat(ctx, ref)
}

func (s tracedStore) Delete(ctx context.Context, ref domain.ResultRef) (err error) {
	// GC calls this from a background context, so the span comes out as a root.
	// The locator is what makes it worth having on its own: it is the only trace
	// of an object the sweep could not remove.
	ctx, span := telemetry.Tracer().Start(ctx, "storage.delete",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("storage.backend", s.backend),
			attribute.String("result.locator", ref.Locator),
		))
	defer telemetry.EndSpan(span, &err)

	return s.ResultStore.Delete(ctx, ref)
}

// tracedWriteCloser holds the write span open until the result is finalized.
// Close is where S3 and ClickHouse wait for the upload to land (I4), so ending
// the span anywhere earlier would leave out the part that takes the time.
type tracedWriteCloser struct {
	io.WriteCloser
	span    trace.Span
	written int64
}

func (w *tracedWriteCloser) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *tracedWriteCloser) Close() error {
	err := w.WriteCloser.Close()
	w.span.SetAttributes(attribute.Int64("storage.bytes_written", w.written))
	telemetry.EndSpan(w.span, &err)
	return err
}

// tracedReadCloser holds the read span open for the whole download rather than
// for the open, which is the part that takes no time.
type tracedReadCloser struct {
	io.ReadCloser
	span trace.Span
	read int64
}

func (r *tracedReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.read += int64(n)
	return n, err
}

// WriteTo keeps io.Copy on the fast path. Without it the wrapper hides the
// io.WriterTo an *os.File carries, and a local result download loses sendfile.
func (r *tracedReadCloser) WriteTo(w io.Writer) (int64, error) {
	var n int64
	var err error
	if wt, ok := r.ReadCloser.(io.WriterTo); ok {
		n, err = wt.WriteTo(w)
	} else {
		// r.ReadCloser is not an io.WriterTo, so this cannot recurse back here.
		n, err = io.Copy(w, r.ReadCloser)
	}
	r.read += n
	return n, err
}

func (r *tracedReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.span.SetAttributes(attribute.Int64("storage.bytes_read", r.read))
	telemetry.EndSpan(r.span, &err)
	return err
}
