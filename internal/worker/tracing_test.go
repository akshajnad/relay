package worker

import (
	"context"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestJobTraceSpansSubmitToComplete checks that one job produces a single
// trace: submit (client and server) -> relay.enqueue -> relay.dispatch ->
// relay.execute (worker) -> CompleteJob (worker client and server).
func TestJobTraceSpansSubmitToComplete(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })

	c := startCluster(t, nil, nil)
	c.startWorker(t, nil, DefaultHandlers(time.Millisecond), nil)

	ctx, root := tracing.Tracer().Start(context.Background(), "test.client")
	resp, err := c.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	root.End()
	traceID := root.SpanContext().TraceID()
	eventually(t, 5*time.Second, "job to succeed", func() bool {
		got, err := c.jobs.GetJob(context.Background(), &relayv1.GetJobRequest{JobId: resp.JobId})
		return err == nil && got.Job.Status == relayv1.JobStatus_JOB_STATUS_SUCCEEDED
	})

	var spans tracetest.SpanStubs
	eventually(t, 5*time.Second, "the completion's server span", func() bool {
		spans = nil
		for _, s := range exp.GetSpans() {
			if s.SpanContext.TraceID() == traceID {
				spans = append(spans, s)
			}
		}
		return find(spans, "relay.v1.WorkerService/CompleteJob", trace.SpanKindServer) != nil
	})
	byName := func(name string, kind trace.SpanKind) tracetest.SpanStub {
		s := find(spans, name, kind)
		if s == nil {
			var names []string
			for _, x := range spans {
				names = append(names, x.Name+"/"+x.SpanKind.String())
			}
			t.Fatalf("no %s %s span in the job's trace; have %v", kind, name, names)
		}
		return *s
	}
	submitServer := byName("relay.v1.JobService/SubmitJob", trace.SpanKindServer)
	enqueue := byName("relay.enqueue", trace.SpanKindInternal)
	dispatch := byName("relay.dispatch", trace.SpanKindInternal)
	execute := byName("relay.execute", trace.SpanKindInternal)
	completeClient := byName("relay.v1.WorkerService/CompleteJob", trace.SpanKindClient)
	completeServer := byName("relay.v1.WorkerService/CompleteJob", trace.SpanKindServer)

	parentOf := func(child, parent tracetest.SpanStub) {
		t.Helper()
		if child.Parent.SpanID() != parent.SpanContext.SpanID() {
			t.Fatalf("%s's parent is %s, want %s", child.Name, child.Parent.SpanID(), parent.Name)
		}
	}
	parentOf(enqueue, submitServer)
	parentOf(dispatch, enqueue)
	parentOf(execute, dispatch)
	parentOf(completeClient, execute)
	parentOf(completeServer, completeClient)
	if len(dispatch.Links) != 1 {
		t.Fatalf("dispatch should link to the worker's AcquireJob span, links = %v", dispatch.Links)
	}
}

func find(spans tracetest.SpanStubs, name string, kind trace.SpanKind) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name && spans[i].SpanKind == kind {
			return &spans[i]
		}
	}
	return nil
}
