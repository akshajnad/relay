// Package tracing sets up OpenTelemetry for Relay's binaries and carries a
// job's trace across the asynchronous gap between submit and execution.
//
// A job's trace is: SubmitJob (client) -> SubmitJob (coordinator) ->
// relay.enqueue -> ... queue wait ... -> relay.dispatch -> relay.execute
// (worker) -> CompleteJob/FailJob (worker client) -> CompleteJob/FailJob
// (coordinator). The gRPC spans come from otelgrpc, which propagates
// context in request metadata. The jump from enqueue to dispatch crosses a
// queue, so the submit's W3C traceparent is stored with the job and
// restored by whoever handles it next.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// Tracer is Relay's tracer.
func Tracer() trace.Tracer { return otel.Tracer("github.com/akshajnad/relay") }

var propagator = propagation.TraceContext{}

// Setup installs a global tracer provider exporting OTLP over gRPC to
// endpoint (host:port). With an empty endpoint tracing stays a no-op. The
// returned function flushes and shuts the exporter down.
func Setup(ctx context.Context, service, endpoint string, sampleRatio float64) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagator)
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	// Schemaless, so it merges with the SDK's default resource whatever
	// semantic-convention version that one uses.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", service)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// ServerOption instruments a gRPC server.
func ServerOption() grpc.ServerOption { return grpc.StatsHandler(otelgrpc.NewServerHandler()) }

// DialOption instruments a gRPC client.
func DialOption() grpc.DialOption { return grpc.WithStatsHandler(otelgrpc.NewClientHandler()) }

// TraceParent returns the W3C traceparent of the span in ctx ("" if none).
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// WithTraceParent returns ctx carrying the remote span context encoded in
// tp, so spans started from it join that trace.
func WithTraceParent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}
