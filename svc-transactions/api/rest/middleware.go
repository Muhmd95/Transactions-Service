package rest

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"svc-transactions/util/logger"
	"svc-transactions/util/metrics"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

const UserIDContextKey contextKey = "user_id"

// --- Response Writer Wrapper ---
// Captures status code and bytes written for logging and metrics.

type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += n
	return n, err
}

// --- Request Logger Middleware ---
// Wraps every request with structured logging including:
// user_id, method, path, status, duration, bytes_read, bytes_written

func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Count bytes read from the request body
		bytesRead := int64(0)
		if r.Body != nil {
			r.Body = &countingReader{ReadCloser: r.Body, bytesRead: &bytesRead}
		}

		// Wrap the response writer to capture status and bytes written
		recorder := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start)
		log := logger.Ctx(r.Context())

		// Extract user_id from context if set by auth middleware
		userID := ""
		if uid, ok := r.Context().Value(UserIDContextKey).(string); ok {
			userID = uid
		}

		// Choose log level based on status code
		var event *zerolog.Event
		switch {
		case recorder.statusCode >= 500:
			event = log.Error()
		case recorder.statusCode >= 400:
			event = log.Warn()
		default:
			event = log.Info()
		}

		// Build the enriched log entry
		event.
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", recorder.statusCode).
			Dur("duration", duration).
			Int64("bytes_read", bytesRead).
			Int("bytes_written", recorder.bytesWritten).
			Str("remote_addr", r.RemoteAddr).
			Str("user_agent", r.UserAgent())

		if userID != "" {
			event.Str("user_id", userID)
		}

		event.Msg("request completed")
	})
}

// --- Metrics Middleware ---
// Records Prometheus metrics for every HTTP request.

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Count bytes read from the request body
		bytesRead := int64(0)
		if r.Body != nil {
			r.Body = &countingReader{ReadCloser: r.Body, bytesRead: &bytesRead}
		}

		recorder := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(recorder.statusCode)

		metrics.HTTPRequestsTotal.WithLabelValues(r.Method, r.URL.Path, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
		metrics.HTTPRequestBytesRead.WithLabelValues(r.Method, r.URL.Path).Observe(float64(bytesRead))
		metrics.HTTPResponseBytesWritten.WithLabelValues(r.Method, r.URL.Path).Observe(float64(recorder.bytesWritten))
	})
}

// --- Counting Reader ---
// Wraps io.ReadCloser to count bytes consumed from request body.

type countingReader struct {
	io.ReadCloser
	bytesRead *int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.ReadCloser.Read(p)
	*cr.bytesRead += int64(n)
	return n, err
}
