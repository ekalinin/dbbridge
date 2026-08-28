package testutil

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// RecordSpans installs a recording TracerProvider and the W3C propagator as the
// globals for the duration of a test, and returns the recorder holding every
// span that ends while it is in place. The globals are what the transports, the
// service and the registries resolve their tracer through, so there is no way
// to hand them a provider other than this.
func RecordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)

	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	// InitOTel installs this in production; a test that never calls it would
	// otherwise run against the no-op propagator and see no incoming context.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	return recorder
}

// spanWait bounds how long SpanByName waits for a span to be exported.
const spanWait = 10 * time.Second

// SpanByName returns the single recorded span with that name. A query produces
// each of its spans once, so more than one is a test that let two runs bleed
// together rather than something to pick from.
//
// It waits for the span to appear rather than reading the recorder once. A span
// is recorded when it ends, and the execution spans end after the caller has
// been answered: a sync submission returns from the watcher notification inside
// complete(), while query.run still has its deferred End to run. Reading
// immediately made every assertion about those spans a race that only lost on a
// slow machine - CI caught it under -race with db.exec and storage.write
// recorded and their parent query.run not yet.
func SpanByName(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	var found []sdktrace.ReadOnlySpan
	deadline := time.Now().Add(spanWait)
	for {
		found = found[:0]
		for _, span := range recorder.Ended() {
			if span.Name() == name {
				found = append(found, span)
			}
		}
		if len(found) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	switch len(found) {
	case 1:
		return found[0]
	case 0:
		t.Fatalf("no span named %q, recorded: %v", name, SpanNames(recorder))
	default:
		t.Fatalf("%d spans named %q, want exactly one", len(found), name)
	}
	return nil
}

// SpanNames lists the recorded span names, for failure messages.
func SpanNames(recorder *tracetest.SpanRecorder) []string {
	ended := recorder.Ended()
	names := make([]string, 0, len(ended))
	for _, span := range ended {
		names = append(names, span.Name())
	}
	return names
}

// RequireChildOf fails unless child's parent is parent, naming both.
func RequireChildOf(t *testing.T, child, parent sdktrace.ReadOnlySpan) {
	t.Helper()

	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("%s has parent %s, want %s (%s)",
			child.Name(), child.Parent().SpanID(), parent.SpanContext().SpanID(), parent.Name())
	}
	if child.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Errorf("%s is in trace %s, want %s (%s)",
			child.Name(), child.SpanContext().TraceID(), parent.SpanContext().TraceID(), parent.Name())
	}
}

// InjectTraceContext writes a traceparent for a freshly minted remote span into
// header and returns its SpanContext, so a test can assert that the transport
// adopted it as the parent. It writes the header itself rather than going
// through an instrumented client, because what is under test is a transport
// honouring whatever W3C context arrives on the wire.
func InjectTraceContext(header http.Header) trace.SpanContext {
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), remote)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(header))
	return remote
}
