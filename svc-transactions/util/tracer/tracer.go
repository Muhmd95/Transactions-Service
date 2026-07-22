package tracer

import (
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// intialize the tracer locally only without connecting to kibana
// i will pass the service name to the func to know which service this tracer belongs
func InitTracer(serviceName string) (*sdktrace.TracerProvider, error) {
	tp := sdktrace.NewTracerProvider() // i will leave the exporter empty for now
	// i will use kibana later

	otel.SetTracerProvider(tp)
	// this registers the global tracee so that when the otelhttp
	// want to create a span it will use this tracer provider
	return tp, nil
}
