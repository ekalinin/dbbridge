package telemetry

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestEndSpan_MarksTheFailureAndAlwaysEnds: every instrumented call site defers
// this with the address of a named return, so an exit path added later is
// covered without touching the defer.
func TestEndSpan_MarksTheFailureAndAlwaysEnds(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	failed := errors.New("storage is unreachable")
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "ok", err: nil, want: codes.Unset},
		{name: "failed", err: failed, want: codes.Error},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, span := tp.Tracer("test").Start(context.Background(), tc.name)
			err := tc.err
			EndSpan(span, &err)

			var ended sdktrace.ReadOnlySpan
			for _, s := range recorder.Ended() {
				if s.Name() == tc.name {
					ended = s
				}
			}
			if ended == nil {
				t.Fatalf("span %q did not end", tc.name)
			}
			if ended.Status().Code != tc.want {
				t.Errorf("status = %v, want %v", ended.Status().Code, tc.want)
			}
		})
	}
}
