package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts every HTTP request by method, path, and status code.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration tracks the latency distribution of HTTP requests.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency distribution",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// HTTPRequestBytesRead tracks the size of incoming request bodies.
	HTTPRequestBytesRead = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_bytes_read",
			Help:    "Size of HTTP request bodies in bytes",
			Buckets: []float64{0, 100, 500, 1000, 5000, 10000, 50000},
		},
		[]string{"method", "path"},
	)

	// HTTPResponseBytesWritten tracks the size of HTTP response bodies.
	HTTPResponseBytesWritten = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_response_bytes_written",
			Help:    "Size of HTTP response bodies in bytes",
			Buckets: []float64{0, 100, 500, 1000, 5000, 10000, 50000},
		},
		[]string{"method", "path"},
	)

	// MongoOperationDuration tracks MongoDB query latency by operation and collection.
	MongoOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mongodb_operation_duration_seconds",
			Help:    "MongoDB operation latency",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
		[]string{"operation", "collection"},
	)
)
