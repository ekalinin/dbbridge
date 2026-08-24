package db

import (
	"context"

	"github.com/ekalinin/dbbridge/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracing sits between the manager and a driver's pool so the time a query
// spends in the database is a span of its own rather than a share of the opaque
// query.run (spec §11). It is applied by OpenPool, so every registered driver is
// instrumented once instead of four times.
//
// Two spans come out of one execution, because the two halves fail differently:
// db.exec covers submitting the statement, db.rows the pull of the result set,
// which runs interleaved with the write to storage.
type tracedPool struct {
	Pool
	engine string
}

// Traced wraps pool so its executions produce spans. It is exported for the
// callers that build a pool without going through OpenPool.
func Traced(pool Pool, engine string) Pool {
	if pool == nil {
		return nil
	}
	return tracedPool{Pool: pool, engine: engine}
}

// Unwrap returns the pool the driver produced. A caller that needs the concrete
// driver type - a test asserting which implementation an engine maps to - goes
// through this rather than around the registry.
func (p tracedPool) Unwrap() Pool { return p.Pool }

// Unwrap returns the pool underneath the tracing wrapper, or pool itself when
// there is none. It is what a caller uses to get at a concrete driver type.
func Unwrap(pool Pool) Pool {
	if u, ok := pool.(interface{ Unwrap() Pool }); ok {
		return u.Unwrap()
	}
	return pool
}

func (p tracedPool) Exec(ctx context.Context, sql string) (stream RowStream, err error) {
	// The SQL text is not an attribute: it is caller-supplied and can carry the
	// data the query filters on, which is not something a trace should keep.
	execCtx, span := telemetry.Tracer().Start(ctx, "db.exec",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", p.engine)))
	defer telemetry.EndSpan(span, &err)

	stream, err = p.Pool.Exec(execCtx, sql)
	if err != nil {
		return nil, err
	}
	// From ctx, not execCtx: db.exec has ended by the time the first row is
	// pulled, so the two are siblings under the execution rather than a span
	// nested inside one that is already over.
	return newTracedRowStream(ctx, stream, p.engine), nil
}

// tracedRowStream holds a span open for as long as the result set is being
// pulled. It ends at Close, which the manager defers for the whole run.
type tracedRowStream struct {
	RowStream
	span trace.Span
	rows int64
}

func newTracedRowStream(ctx context.Context, stream RowStream, engine string) *tracedRowStream {
	_, span := telemetry.Tracer().Start(ctx, "db.rows",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", engine)))
	return &tracedRowStream{RowStream: stream, span: span}
}

func (s *tracedRowStream) Next() bool {
	ok := s.RowStream.Next()
	if ok {
		s.rows++
	}
	return ok
}

func (s *tracedRowStream) Close() error {
	err := s.RowStream.Close()
	// The span reports the row error as well: a stream that stopped early says
	// why in Err(), and that is the database-side failure worth seeing here.
	// What Close returns is left untouched - the caller logs it as a close
	// failure, which a mid-stream error is not.
	spanErr := err
	if spanErr == nil {
		spanErr = s.Err()
	}
	s.span.SetAttributes(attribute.Int64("db.rows_read", s.rows))
	telemetry.EndSpan(s.span, &spanErr)
	return err
}
