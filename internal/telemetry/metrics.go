package telemetry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	QueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dbbridge_queries_total",
			Help: "Total number of queries submitted, partitioned by engine and final execution state.",
		},
		[]string{"engine", "state"},
	)

	QueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dbbridge_query_duration_seconds",
			Help:    "Execution duration of SQL queries in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"engine"},
	)

	InflightQueries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "dbbridge_inflight_queries",
			Help: "Number of active queries currently executing on this instance.",
		},
	)

	ResultBytesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dbbridge_result_bytes_total",
			Help: "Total bytes of query results serialized and saved to result storage.",
		},
		[]string{"backend"},
	)

	IdempotencyHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "dbbridge_idempotency_hits_total",
			Help: "Total number of duplicate requests that were resolved via idempotency checks.",
		},
	)
)

func init() {
	prometheus.MustRegister(QueriesTotal)
	prometheus.MustRegister(QueryDuration)
	prometheus.MustRegister(InflightQueries)
	prometheus.MustRegister(ResultBytesTotal)
	prometheus.MustRegister(IdempotencyHitsTotal)
}

// RecordQueryCompleted records stats when a query succeeds, fails or gets canceled.
func RecordQueryCompleted(engine string, state string, duration time.Duration) {
	QueriesTotal.WithLabelValues(engine, state).Inc()
	QueryDuration.WithLabelValues(engine).Observe(duration.Seconds())
}

// RecordQueryStarted registers stats when a query goes from pending to running.
func RecordQueryStarted() {
	InflightQueries.Inc()
}

// RecordQueryFinished decrements the inflight query gauge.
func RecordQueryFinished() {
	InflightQueries.Dec()
}

// RecordResultBytes tracks the size of written query results.
func RecordResultBytes(backend string, size int64) {
	ResultBytesTotal.WithLabelValues(backend).Add(float64(size))
}

// RecordIdempotencyHit increments the duplicate request counter.
func RecordIdempotencyHit() {
	IdempotencyHitsTotal.Inc()
}

// Handler returns the Prometheus HTTP handler for scraping metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
