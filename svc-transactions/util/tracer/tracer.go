package tracer

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracer initializes the OpenTelemetry TracerProvider with an OTLP HTTP exporter.
// Traces are sent to Jaeger (or any OTLP-compatible backend) at the given endpoint.
// If otlpEndpoint is empty, a local-only tracer is created (no export).
func InitTracer(serviceName string, otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	var opts []sdktrace.TracerProviderOption

	// Set the service resource so spans are tagged with the service name
	opts = append(opts, sdktrace.WithResource(resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
	)))

	// If an OTLP endpoint is provided, create an exporter that sends traces to Jaeger
	if otlpEndpoint != "" {
		exporter, err := otlptracehttp.New(
			context.Background(),
			otlptracehttp.WithEndpoint(otlpEndpoint),
			otlptracehttp.WithInsecure(), // no TLS in dev/docker network
		)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(opts...)

	// Register globally so otelhttp and otelgrpc auto-instrument using this provider
	otel.SetTracerProvider(tp)

	// Set the propagator so trace context (traceparent header) is injected/extracted
	// across HTTP and gRPC boundaries between services
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}
