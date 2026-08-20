package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// OTelShutdown represents a function to shut down the OTel providers gracefully.
type OTelShutdown func(context.Context) error

// InitOTel initializes OpenTelemetry tracing and metrics.
//
// The meter provider is installed even without an OTLP endpoint: Prometheus is
// one of its readers, so /metrics is served from the same instruments rather
// than from a second, parallel metrics stack.
func InitOTel(ctx context.Context, serviceName, otlpEndpoint string) (OTelShutdown, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	mp, err := ensureMeterProvider(res, otlpEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	if otlpEndpoint == "" {
		// No tracing backend configured, and nothing to flush: the Prometheus
		// reader is pull-based. Shutting the provider down here would only stop
		// /metrics from being updated.
		return func(context.Context) error { return nil }, nil
	}

	// 1. Trace Exporter & Provider
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	shutdown := func(shutdownCtx context.Context) error {
		var errs []error
		if err := tp.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("tracer provider shutdown failed: %w", err))
		}
		if err := mp.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("meter provider shutdown failed: %w", err))
		}
		if len(errs) > 0 {
			return fmt.Errorf("OTel shutdown encountered errors: %v", errs)
		}
		return nil
	}

	return shutdown, nil
}

// newOTLPMetricReader builds the periodic OTLP reader that ensureMeterProvider
// adds alongside the Prometheus one.
func newOTLPMetricReader(ctx context.Context, otlpEndpoint string) (sdkmetric.Reader, error) {
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(otlpEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}
	return sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(15*time.Second)), nil
}
