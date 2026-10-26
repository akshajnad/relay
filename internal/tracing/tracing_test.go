package tracing

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestSetupWithEndpoint builds the real exporter pipeline. The exporter
// connects lazily, so no collector is needed.
func TestSetupWithEndpoint(t *testing.T) {
	shutdown, err := Setup(context.Background(), "relay-test", "127.0.0.1:1", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	ctx, span := Tracer().Start(context.Background(), "x")
	tp := TraceParent(ctx)
	span.End()
	if tp == "" {
		t.Fatal("no traceparent for a sampled span")
	}
	// The traceparent round-trips into a context a child can join.
	_, child := Tracer().Start(WithTraceParent(context.Background(), tp), "y")
	if child.SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Fatal("child did not join the parent's trace")
	}
	child.End()
	sctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = shutdown(sctx) // export fails (no collector); shutdown must still return
}

func TestSetupWithoutEndpointIsNoop(t *testing.T) {
	shutdown, err := Setup(context.Background(), "relay-test", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if TraceParent(context.Background()) != "" {
		t.Fatal("traceparent without a span")
	}
}
