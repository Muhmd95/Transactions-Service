package logger

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

// Log is the globally accessible logger instance
var Log zerolog.Logger

func InitLogger(serviceName string) {
	env := os.Getenv("APP_ENV") // "development" | "production"

	var log zerolog.Logger

	if env == "production" {
		// Production: JSON output, INFO level, no color
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		log = zerolog.New(os.Stdout).
			With().
			Timestamp().
			Str("service", serviceName).
			Logger()
	} else {
		// Development: pretty console, DEBUG level
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		consoleWriter := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}
		log = zerolog.New(consoleWriter).
			With().
			Timestamp().
			Str("service", serviceName).
			Logger()
	}

	Log = log
}

// Ctx extracts the OpenTelemetry Trace ID from the context and attaches it to the logger.
// This correlates logs with Jaeger traces for end-to-end request debugging.
func Ctx(ctx context.Context) zerolog.Logger {
	spanContext := trace.SpanFromContext(ctx).SpanContext()

	if !spanContext.IsValid() {
		return Log
	}

	return Log.With().
		Str("trace_id", spanContext.TraceID().String()).
		Str("span_id", spanContext.SpanID().String()).
		Logger()
}
