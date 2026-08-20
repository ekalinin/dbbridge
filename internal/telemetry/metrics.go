package telemetry

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// The domain metrics used to live on prometheus/client_golang while an OTel
// MeterProvider was raised beside them and never given a single instrument, so
// nothing reached OTLP. There is one source of truth now: the instruments are
// OTel, and Prometheus is one of the readers behind them, which keeps /metrics
// serving the same metric names. Go runtime metrics stay on the Prometheus
// collector, which is what exposes the full runtime/metrics ruleset (spec §11).

const meterName = "github.com/ekalinin/dbbridge"

// meter is the global delegating meter. Instruments created from it before the
// real provider is installed are rebound when InitOTel sets one, so package
// initialization order does not matter.
var meter = otel.Meter(meterName)

var (
	queriesTotal         = mustInt64Counter("dbbridge_queries_total", "Total number of queries submitted, partitioned by engine and final execution state.")
	resultBytesTotal     = mustInt64Counter("dbbridge_result_bytes_total", "Total bytes of query results serialized and saved to result storage.")
	idempotencyHitsTotal = mustInt64Counter("dbbridge_idempotency_hits_total", "Total number of duplicate requests that were resolved via idempotency checks.")

	queryDuration = mustFloat64Histogram("dbbridge_query_duration_seconds", "s", "Execution duration of SQL queries in seconds.")

	inflightQueries = mustInt64UpDownCounter("dbbridge_inflight_queries", "Number of active queries currently executing on this instance.")

	dbPoolStats = mustInt64Gauge("dbbridge_db_pool_stats", "Database connection pool statistics (open/idle/in_use connections) per database.")
)

func mustInt64Counter(name, desc string) metric.Int64Counter {
	c, err := meter.Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		log.Printf("ERROR: failed to create counter %s: %v", name, err)
	}
	return c
}

func mustInt64UpDownCounter(name, desc string) metric.Int64UpDownCounter {
	c, err := meter.Int64UpDownCounter(name, metric.WithDescription(desc))
	if err != nil {
		log.Printf("ERROR: failed to create up-down counter %s: %v", name, err)
	}
	return c
}

func mustInt64Gauge(name, desc string) metric.Int64Gauge {
	g, err := meter.Int64Gauge(name, metric.WithDescription(desc))
	if err != nil {
		log.Printf("ERROR: failed to create gauge %s: %v", name, err)
	}
	return g
}

func mustFloat64Histogram(name, unit, desc string) metric.Float64Histogram {
	h, err := meter.Float64Histogram(name, metric.WithUnit(unit), metric.WithDescription(desc))
	if err != nil {
		log.Printf("ERROR: failed to create histogram %s: %v", name, err)
	}
	return h
}

func init() {
	// Spec §11: export Go runtime metrics via the runtime/metrics package.
	// The default registry pre-registers a legacy (MemStats-based) Go collector;
	// replace it with one backed by the full runtime/metrics ruleset.
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(
			collectors.MetricsAll,
		),
	))
}

var (
	providerMu       sync.Mutex
	providerInstance *sdkmetric.MeterProvider
)

// ensureMeterProvider installs the global MeterProvider on first use. The
// Prometheus reader is always present so /metrics keeps working even when no
// OTLP endpoint is configured; the OTLP reader is added when there is one.
func ensureMeterProvider(res *resource.Resource, otlpEndpoint string) (*sdkmetric.MeterProvider, error) {
	providerMu.Lock()
	defer providerMu.Unlock()

	if providerInstance != nil {
		// Whoever got here first decided the configuration. Silently dropping a
		// resource or an OTLP endpoint is exactly the failure this package is
		// meant to prevent: a metrics stack that exports nothing and says so
		// nowhere, with target_info missing its service.name.
		if res != nil || otlpEndpoint != "" {
			return nil, errors.New("the meter provider was already built without them; call InitOTel before serving /metrics")
		}
		return providerInstance, nil
	}

	// The OTLP reader is built first. otelprom.New registers a collector with
	// the default registry as a side effect and hands back no way to remove it,
	// so failing after that call left the collector behind; the collector is
	// unchecked, so the next attempt registered a second one and /metrics then
	// answered 500 on duplicated series.
	var otlpReader sdkmetric.Reader
	if otlpEndpoint != "" {
		reader, err := newOTLPMetricReader(context.Background(), otlpEndpoint)
		if err != nil {
			return nil, err
		}
		otlpReader = reader
	}

	promExporter, err := otelprom.New(
		// Into the default registry, which promhttp.Handler serves.
		otelprom.WithRegisterer(prometheus.DefaultRegisterer),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, err
	}

	opts := []sdkmetric.Option{
		sdkmetric.WithReader(promExporter),
		// Keep the bucket layout the Prometheus client used, so existing
		// dashboards and alerts on this histogram keep working.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "dbbridge_query_duration_seconds"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}},
		)),
	}
	if res != nil {
		opts = append(opts, sdkmetric.WithResource(res))
	}
	if otlpReader != nil {
		opts = append(opts, sdkmetric.WithReader(otlpReader))
	}

	providerInstance = sdkmetric.NewMeterProvider(opts...)
	otel.SetMeterProvider(providerInstance)
	return providerInstance, nil
}

// RecordQueryCompleted records stats when a query succeeds, fails or gets canceled.
func RecordQueryCompleted(engine string, state string, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("engine", engine),
		attribute.String("state", state),
	)
	queriesTotal.Add(context.Background(), 1, attrs)
	queryDuration.Record(context.Background(), duration.Seconds(),
		metric.WithAttributes(attribute.String("engine", engine)))
}

// RecordQueryStarted registers stats when a query goes from pending to running.
func RecordQueryStarted() {
	inflightQueries.Add(context.Background(), 1)
}

// RecordQueryFinished decrements the inflight query gauge.
func RecordQueryFinished() {
	inflightQueries.Add(context.Background(), -1)
}

// RecordResultBytes tracks the size of written query results.
func RecordResultBytes(backend string, size int64) {
	resultBytesTotal.Add(context.Background(), size,
		metric.WithAttributes(attribute.String("backend", backend)))
}

// RecordIdempotencyHit increments the duplicate request counter.
func RecordIdempotencyHit() {
	idempotencyHitsTotal.Add(context.Background(), 1)
}

// RecordPoolStat updates the connection pool gauge for a single database.
func RecordPoolStat(dbID string, open, idle, inUse int32) {
	ctx := context.Background()
	for stat, value := range map[string]int32{"open": open, "idle": idle, "in_use": inUse} {
		dbPoolStats.Record(ctx, int64(value), metric.WithAttributes(
			attribute.String("db_id", dbID),
			attribute.String("stat", stat),
		))
	}
}

// Handler returns the Prometheus HTTP handler for scraping metrics.
func Handler() http.Handler {
	// Serving /metrics without a provider would expose Go runtime metrics only,
	// so make sure the Prometheus reader is wired up even if InitOTel was never
	// called (which is the case in tests and embedded use).
	if _, err := ensureMeterProvider(nil, ""); err != nil {
		log.Printf("ERROR: failed to initialize the meter provider: %v", err)
	}
	return promhttp.Handler()
}
