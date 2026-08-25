package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope every dbbridge span is emitted under.
// The transports are the exception: otelhttp and otelconnect carry their own
// scope, which is how a collector tells library instrumentation from ours.
const tracerName = "dbbridge"

// Tracer returns the tracer the transport, service, manager, driver and store
// layers all start their spans from (spec §11). It resolves through the global
// provider on every call, so a span started before InitOTel installed one is a
// no-op rather than a span bound to a provider nobody exports.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// EndSpan ends span, marking it failed when *err is non-nil. It takes the
// address of a named error return so a single deferred call covers every exit
// path, including the ones added later.
func EndSpan(span trace.Span, err *error) {
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
	}
	span.End()
}
